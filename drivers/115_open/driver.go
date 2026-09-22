package _115_open

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	stdpath "path"
	"slices"
	"strconv"
	"strings"
	"time"

	sdk "github.com/OpenListTeam/115-sdk-go"
	"github.com/OpenListTeam/OpenList/v4/cmd/flags"
	"github.com/OpenListTeam/OpenList/v4/drivers/base"
	"github.com/OpenListTeam/OpenList/v4/internal/driver"
	"github.com/OpenListTeam/OpenList/v4/internal/errs"
	"github.com/OpenListTeam/OpenList/v4/internal/model"
	"github.com/OpenListTeam/OpenList/v4/internal/op"
	"github.com/OpenListTeam/OpenList/v4/internal/stream"
	"github.com/OpenListTeam/OpenList/v4/pkg/http_range"
	"github.com/OpenListTeam/OpenList/v4/pkg/utils"
	"golang.org/x/time/rate"
)

type Open115 struct {
	model.Storage
	Addition
	client     *sdk.Client
	limiter    *rate.Limiter
	parentPath string
}

func (d *Open115) Config() driver.Config {
	return config
}

func (d *Open115) GetAddition() driver.Additional {
	return &d.Addition
}

func (d *Open115) Init(ctx context.Context) error {
	d.client = sdk.New(sdk.WithRefreshToken(d.Addition.RefreshToken),
		sdk.WithAccessToken(d.Addition.AccessToken),
		sdk.WithOnRefreshToken(func(s1, s2 string) {
			d.Addition.AccessToken = s1
			d.Addition.RefreshToken = s2
			op.MustSaveDriverStorage(d)
		}))
	if flags.Debug || flags.Dev {
		d.client.SetDebug(true)
	}
	_, err := d.client.UserInfo(ctx)
	if err != nil {
		return err
	}
	if d.Addition.LimitRate > 0 {
		d.limiter = rate.NewLimiter(rate.Limit(d.Addition.LimitRate), 1)
	}
	if d.PageSize <= 0 {
		d.PageSize = 200
	} else if d.PageSize > 1150 {
		d.PageSize = 1150
	}

	// add parent path
	d.parentPath = "/"
	if d.GetRootId() != d.Config().DefaultRoot {
		folderInfo, err := d.client.GetFolderInfo(ctx, d.GetRootId())
		if err != nil {
			return err
		}

		if folderInfo.FileID != d.Config().DefaultRoot {
			d.parentPath = stdpath.Join(d.parentPath, folderInfo.FileName)
		}

		parentPaths := folderInfo.Paths
		slices.Reverse(parentPaths)
		for _, parentPathInfo := range parentPaths {
			if parentPathInfo.FileID == d.Config().DefaultRoot {
				d.parentPath = stdpath.Join("/", d.parentPath)
			} else {
				d.parentPath = stdpath.Join("/", parentPathInfo.FileName, d.parentPath)
			}
		}
	}
	return nil
}

func (d *Open115) WaitLimit(ctx context.Context) error {
	if d.limiter != nil {
		return d.limiter.Wait(ctx)
	}
	return nil
}

func (d *Open115) Drop(ctx context.Context) error {
	return nil
}

func (d *Open115) List(ctx context.Context, dir model.Obj, args model.ListArgs) ([]model.Obj, error) {
	var res []model.Obj
	pageSize := int64(d.PageSize)
	offset := int64(0)
	for {
		if err := d.WaitLimit(ctx); err != nil {
			return nil, err
		}
		resp, err := d.client.GetFiles(ctx, &sdk.GetFilesReq{
			CID:    dir.GetID(),
			Limit:  pageSize,
			Offset: offset,
			ASC:    d.Addition.OrderDirection == "asc",
			O:      d.Addition.OrderBy,
			// Cur:     1,
			ShowDir: true,
		})
		if err != nil {
			return nil, err
		}
		res = append(res, utils.MustSliceConvert(resp.Data, func(src sdk.GetFilesResp_File) model.Obj {
			obj := Obj(src)
			return &obj
		})...)
		if len(res) >= int(resp.Count) {
			break
		}
		offset += pageSize
	}
	return res, nil
}

func (d *Open115) Link(ctx context.Context, file model.Obj, args model.LinkArgs) (*model.Link, error) {
	if err := d.WaitLimit(ctx); err != nil {
		return nil, err
	}
	var ua string
	if args.Header != nil {
		ua = args.Header.Get("User-Agent")
	}
	if ua == "" {
		ua = base.UserAgent
	}
	obj, ok := file.(*Obj)
	if !ok {
		return nil, fmt.Errorf("can't convert obj")
	}
	pc := obj.Pc
	resp, err := d.client.DownURL(ctx, pc, ua)
	if err != nil {
		return nil, err
	}
	u, ok := resp[obj.GetID()]
	if !ok {
		return nil, fmt.Errorf("can't get link")
	}
	return &model.Link{
		URL: u.URL.URL,
		Header: http.Header{
			"User-Agent": []string{ua},
		},
	}, nil
}

func (d *Open115) Get(ctx context.Context, path string) (model.Obj, error) {
	if err := d.WaitLimit(ctx); err != nil {
		return nil, err
	}
	path = stdpath.Join(d.parentPath, path)
	resp, err := d.client.GetFolderInfoByPath(ctx, path)
	if err != nil {
		if errors.Is(err, sdk.ErrObjectNotFound) {
			return d.getFromParent(ctx, path, "")
		}
		return nil, err
	}
	obj := &Obj{
		Fid:  resp.FileID,
		Fn:   resp.FileName,
		Fc:   resp.FileCategory,
		Sha1: resp.Sha1,
		Pc:   resp.PickCode,
		FS:   resp.SizeByte,
		Upt:  parseTime(resp.UTime),
		UpPt: parseTime(resp.PTime),
	}
	if !obj.IsDir() && (obj.GetSize() <= 0 || obj.ModTime().Unix() <= 0) {
		// 115 Open can briefly return incomplete metadata immediately after an
		// upload (notably size=0 and/or epoch timestamps). The parent listing is
		// more reliable in that window and is also safe for legitimate empty files.
		return d.getFromParent(ctx, path, obj.GetID())
	}
	return obj, nil
}

func (d *Open115) getFromParent(ctx context.Context, path, id string) (model.Obj, error) {
	path = stdpath.Clean(path)
	parent, name := stdpath.Split(path)
	parent = stdpath.Clean(parent)
	parentID := d.GetRootId()
	if stdpath.Clean(parent) != stdpath.Clean(d.parentPath) {
		if err := d.WaitLimit(ctx); err != nil {
			return nil, err
		}
		parentInfo, err := d.client.GetFolderInfoByPath(ctx, parent)
		if err != nil {
			if !errors.Is(err, sdk.ErrObjectNotFound) {
				return nil, err
			}
			parentObj, err := d.getFromParent(ctx, parent, "")
			if err != nil {
				return nil, err
			}
			parentID = parentObj.GetID()
		} else {
			parentID = parentInfo.FileID
		}
	}
	files, err := d.List(ctx, &Obj{Fid: parentID, Fc: "0"}, model.ListArgs{})
	if err != nil {
		return nil, err
	}
	for _, file := range files {
		if (id != "" && file.GetID() == id) || (id == "" && file.GetName() == name) {
			return file, nil
		}
	}
	return nil, errs.ObjectNotFound
}

func (d *Open115) MakeDir(ctx context.Context, parentDir model.Obj, dirName string) (model.Obj, error) {
	if err := d.WaitLimit(ctx); err != nil {
		return nil, err
	}
	resp, err := d.client.Mkdir(ctx, parentDir.GetID(), dirName)
	if err != nil {
		return nil, err
	}
	return &Obj{
		Fid:  resp.FileID,
		Pid:  parentDir.GetID(),
		Fn:   dirName,
		Fc:   "0",
		Upt:  time.Now().Unix(),
		Uet:  time.Now().Unix(),
		UpPt: time.Now().Unix(),
	}, nil
}

func (d *Open115) Move(ctx context.Context, srcObj, dstDir model.Obj) error {
	if err := d.WaitLimit(ctx); err != nil {
		return err
	}
	_, err := d.client.Move(ctx, &sdk.MoveReq{
		FileIDs: srcObj.GetID(),
		ToCid:   dstDir.GetID(),
	})
	return err
}

func (d *Open115) Rename(ctx context.Context, srcObj model.Obj, newName string) (model.Obj, error) {
	if err := d.WaitLimit(ctx); err != nil {
		return nil, err
	}
	_, err := d.client.UpdateFile(ctx, &sdk.UpdateFileReq{
		FileID:   srcObj.GetID(),
		FileName: newName,
	})
	if err != nil {
		return nil, err
	}
	obj, ok := srcObj.(*Obj)
	if ok {
		obj.Fn = newName
		return srcObj, nil
	}
	return nil, nil
}

func (d *Open115) Copy(ctx context.Context, srcObj, dstDir model.Obj) error {
	if err := d.WaitLimit(ctx); err != nil {
		return err
	}
	_, err := d.client.Copy(ctx, &sdk.CopyReq{
		PID:     dstDir.GetID(),
		FileID:  srcObj.GetID(),
		NoDupli: "1",
	})
	return err
}

func (d *Open115) Remove(ctx context.Context, obj model.Obj) error {
	if err := d.WaitLimit(ctx); err != nil {
		return err
	}
	_obj, ok := obj.(*Obj)
	if !ok {
		return fmt.Errorf("can't convert obj")
	}
	_, err := d.client.DelFile(ctx, &sdk.DelFileReq{
		FileIDs:  _obj.GetID(),
		ParentID: _obj.Pid,
	})
	if err != nil {
		return err
	}
	return nil
}

func parseSignCheckRange(signCheck string, fileSize int64) (start, length int64, err error) {
	if fileSize < 0 {
		return 0, 0, fmt.Errorf("invalid file size %d", fileSize)
	}
	parts := strings.Split(signCheck, "-")
	if len(parts) != 2 || parts[0] == "" || parts[1] == "" {
		return 0, 0, fmt.Errorf("invalid sign_check %q", signCheck)
	}
	start, err = strconv.ParseInt(parts[0], 10, 64)
	if err != nil {
		return 0, 0, fmt.Errorf("invalid sign_check start %q: %w", parts[0], err)
	}
	end, err := strconv.ParseInt(parts[1], 10, 64)
	if err != nil {
		return 0, 0, fmt.Errorf("invalid sign_check end %q: %w", parts[1], err)
	}
	if start < 0 || end < start || start >= fileSize || end >= fileSize {
		return 0, 0, fmt.Errorf("sign_check range %q is outside file size %d", signCheck, fileSize)
	}
	return start, end - start + 1, nil
}

func (d *Open115) Put(ctx context.Context, dstDir model.Obj, file model.FileStreamer, up driver.UpdateProgress) error {
	if err := d.WaitLimit(ctx); err != nil {
		return fmt.Errorf("115 Open rate limit: %w", err)
	}
	fileSize := file.GetSize()
	if fileSize < 0 {
		return fmt.Errorf("115 Open upload has invalid file size %d", fileSize)
	}

	sha1 := file.GetHash().GetHash(utils.SHA1)
	if len(sha1) != utils.SHA1.Width {
		var err error
		_, sha1, err = stream.CacheFullAndHash(file, &up, utils.SHA1)
		if err != nil {
			return fmt.Errorf("115 Open full-file SHA1: %w", err)
		}
	}

	const preHashSize int64 = 128 * utils.KB
	hashSize := min(preHashSize, fileSize)
	reader, err := file.RangeRead(http_range.Range{Start: 0, Length: hashSize})
	if err != nil {
		return fmt.Errorf("115 Open prehash range read (file_size=%d length=%d): %w", fileSize, hashSize, err)
	}
	sha1128k, err := utils.HashReader(utils.SHA1, reader)
	if err != nil {
		return fmt.Errorf("115 Open prehash SHA1 (file_size=%d length=%d): %w", fileSize, hashSize, err)
	}

	resp, err := d.client.UploadInit(ctx, &sdk.UploadInitReq{
		FileName: file.GetName(),
		FileSize: fileSize,
		Target:   dstDir.GetID(),
		FileID:   strings.ToUpper(sha1),
		PreID:    strings.ToUpper(sha1128k),
	})
	if err != nil {
		return fmt.Errorf("115 Open upload init: %w", err)
	}
	if resp.Status == 2 {
		up(100)
		return nil
	}

	if utils.SliceContains([]int{6, 7, 8}, resp.Status) {
		start, length, rangeErr := parseSignCheckRange(resp.SignCheck, fileSize)
		if rangeErr != nil {
			return fmt.Errorf("115 Open sign_check validation: %w", rangeErr)
		}
		reader, err = file.RangeRead(http_range.Range{Start: start, Length: length})
		if err != nil {
			return fmt.Errorf(
				"115 Open sign_check range read (sign_check=%q start=%d length=%d file_size=%d): %w",
				resp.SignCheck, start, length, fileSize, err,
			)
		}
		signVal, hashErr := utils.HashReader(utils.SHA1, reader)
		if hashErr != nil {
			return fmt.Errorf(
				"115 Open sign_check SHA1 (sign_check=%q start=%d length=%d file_size=%d): %w",
				resp.SignCheck, start, length, fileSize, hashErr,
			)
		}
		resp, err = d.client.UploadInit(ctx, &sdk.UploadInitReq{
			FileName: file.GetName(),
			FileSize: fileSize,
			Target:   dstDir.GetID(),
			FileID:   strings.ToUpper(sha1),
			PreID:    strings.ToUpper(sha1128k),
			SignKey:  resp.SignKey,
			SignVal:  strings.ToUpper(signVal),
		})
		if err != nil {
			return fmt.Errorf("115 Open sign_check upload init: %w", err)
		}
		if resp.Status == 2 {
			up(100)
			return nil
		}
	}

	tokenResp, err := d.client.UploadGetToken(ctx)
	if err != nil {
		return fmt.Errorf("115 Open upload token: %w", err)
	}
	if err := d.multpartUpload(ctx, file, up, tokenResp, resp); err != nil {
		return fmt.Errorf("115 Open multipart upload: %w", err)
	}
	return nil
}

func (d *Open115) OfflineDownload(ctx context.Context, uris []string, dstDir model.Obj) ([]string, error) {
	return d.client.AddOfflineTaskURIs(ctx, uris, dstDir.GetID())
}

func (d *Open115) DeleteOfflineTask(ctx context.Context, infoHash string, deleteFiles bool) error {
	return d.client.DeleteOfflineTask(ctx, infoHash, deleteFiles)
}

func (d *Open115) OfflineList(ctx context.Context) (*sdk.OfflineTaskListResp, error) {
	resp, err := d.client.OfflineTaskList(ctx, 1)
	if err != nil {
		return nil, err
	}
	return resp, nil
}

func (d *Open115) GetDetails(ctx context.Context) (*model.StorageDetails, error) {
	userInfo, err := d.client.UserInfo(ctx)
	if err != nil {
		return nil, err
	}
	total, err := ParseInt64(userInfo.RtSpaceInfo.AllTotal.Size)
	if err != nil {
		return nil, err
	}
	used, err := ParseInt64(userInfo.RtSpaceInfo.AllUse.Size)
	if err != nil {
		return nil, err
	}
	return &model.StorageDetails{
		DiskUsage: model.DiskUsage{
			TotalSpace: total,
			UsedSpace:  used,
		},
	}, nil
}

// func (d *Open115) GetArchiveMeta(ctx context.Context, obj model.Obj, args model.ArchiveArgs) (model.ArchiveMeta, error) {
// 	// TODO get archive file meta-info, return errs.NotImplement to use an internal archive tool, optional
// 	return nil, errs.NotImplement
// }

// func (d *Open115) ListArchive(ctx context.Context, obj model.Obj, args model.ArchiveInnerArgs) ([]model.Obj, error) {
// 	// TODO list args.InnerPath in the archive obj, return errs.NotImplement to use an internal archive tool, optional
// 	return nil, errs.NotImplement
// }

// func (d *Open115) Extract(ctx context.Context, obj model.Obj, args model.ArchiveInnerArgs) (*model.Link, error) {
// 	// TODO return link of file args.InnerPath in the archive obj, return errs.NotImplement to use an internal archive tool, optional
// 	return nil, errs.NotImplement
// }

// func (d *Open115) ArchiveDecompress(ctx context.Context, srcObj, dstDir model.Obj, args model.ArchiveDecompressArgs) ([]model.Obj, error) {
// 	// TODO extract args.InnerPath path in the archive srcObj to the dstDir location, optional
// 	// a folder with the same name as the archive file needs to be created to store the extracted results if args.PutIntoNewDir
// 	// return errs.NotImplement to use an internal archive tool
// 	return nil, errs.NotImplement
// }

//func (d *Template) Other(ctx context.Context, args model.OtherArgs) (interface{}, error) {
//	return nil, errs.NotSupport
//}

var _ driver.Driver = (*Open115)(nil)
