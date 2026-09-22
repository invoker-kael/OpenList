package handles

import (
	"net/http"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/OpenListTeam/OpenList/v4/internal/conf"
	"github.com/OpenListTeam/OpenList/v4/internal/db"
	"github.com/OpenListTeam/OpenList/v4/internal/model"
	"github.com/OpenListTeam/OpenList/v4/internal/writeback"
	"github.com/OpenListTeam/OpenList/v4/server/common"
	"github.com/gin-gonic/gin"
)

const (
	webDAVMonitorDefaultLimit = 100
	webDAVMonitorMaxLimit     = 500
)

type webDAVWritebackStateSummary struct {
	Count int64 `json:"count"`
	Bytes int64 `json:"bytes"`
}

type webDAVWritebackSummary struct {
	Enabled                  bool                                   `json:"enabled"`
	Workers                  int                                    `json:"workers"`
	Receiving                int64                                  `json:"receiving"`
	ReceivingExpectedBytes   int64                                  `json:"receiving_expected_bytes"`
	BacklogBytes             int64                                  `json:"backlog_bytes"`
	CompletedCacheBytes      int64                                  `json:"completed_cache_bytes"`
	MaxPendingSpoolBytes     uint64                                 `json:"max_pending_spool_bytes"`
	ReserveFreeSpaceBytes    uint64                                 `json:"reserve_free_space_bytes"`
	CompletedCacheTTLMinutes int                                    `json:"completed_cache_ttl_minutes"`
	Errors                   int64                                  `json:"errors"`
	States                   map[string]webDAVWritebackStateSummary `json:"states"`
	UpdatedAt                time.Time                              `json:"updated_at"`
}

type webDAVWritebackCleanupResult struct {
	Released int `json:"released"`
}

type webDAVWritebackStateAggregate struct {
	State string
	Count int64
	Bytes int64
}

type webDAVWritebackMonitorRow struct {
	ID               string     `json:"id"`
	Path             string     `json:"path"`
	Name             string     `json:"name"`
	IsDir            bool       `json:"is_dir"`
	Size             int64      `json:"size"`
	ClientState      string     `json:"client_state"`
	ProviderState    string     `json:"provider_state"`
	Generation       uint64     `json:"generation"`
	RemoteGeneration uint64     `json:"remote_generation"`
	PayloadSHA1      string     `json:"payload_sha1"`
	RemoteSHA1       string     `json:"remote_sha1"`
	RetryCount       int        `json:"retry_count"`
	VerifyCount      int        `json:"verify_count"`
	ActiveReceivers  int        `json:"active_receivers,omitempty"`
	LastError        string     `json:"last_error"`
	RetryAt          *time.Time `json:"retry_at"`
	RemoteVerifiedAt *time.Time `json:"remote_verified_at"`
	CompletedAt      *time.Time `json:"completed_at"`
	StartedAt        *time.Time `json:"started_at,omitempty"`
	CreatedAt        time.Time  `json:"created_at"`
	UpdatedAt        time.Time  `json:"updated_at"`
}

func WebDAVWritebackMonitorSummary(c *gin.Context) {
	now := time.Now()
	states := make(map[string]webDAVWritebackStateSummary)
	var aggregates []webDAVWritebackStateAggregate
	if err := db.GetDb().
		Model(&model.WebDAVWritebackObject{}).
		Select("state, COUNT(*) AS count, COALESCE(SUM(size), 0) AS bytes").
		Group("state").
		Scan(&aggregates).Error; err != nil {
		common.ErrorResp(c, err, http.StatusInternalServerError)
		return
	}
	for _, aggregate := range aggregates {
		states[aggregate.State] = webDAVWritebackStateSummary{
			Count: aggregate.Count,
			Bytes: aggregate.Bytes,
		}
	}

	var receiving int64
	if err := db.GetDb().
		Model(&model.WebDAVWritebackReceiveFence{}).
		Where("active_receivers > 0 AND receive_lease_until > ?", now).
		Count(&receiving).Error; err != nil {
		common.ErrorResp(c, err, http.StatusInternalServerError)
		return
	}

	var receivingExpectedBytes int64
	if err := db.GetDb().
		Model(&model.WebDAVWritebackReceiveFence{}).
		Where("active_receivers > 0 AND receive_lease_until > ?", now).
		Select("COALESCE(SUM(latest_expected_size), 0)").
		Scan(&receivingExpectedBytes).Error; err != nil {
		common.ErrorResp(c, err, http.StatusInternalServerError)
		return
	}

	var backlogBytes int64
	if err := db.GetDb().
		Model(&model.WebDAVWritebackObject{}).
		Where("state IN ? AND spool_path <> ''", []string{writeback.StateQueued, writeback.StateUploading, writeback.StateVerifying}).
		Select("COALESCE(SUM(size), 0)").
		Scan(&backlogBytes).Error; err != nil {
		common.ErrorResp(c, err, http.StatusInternalServerError)
		return
	}

	var completedCacheBytes int64
	if err := db.GetDb().
		Model(&model.WebDAVWritebackObject{}).
		Where("state = ? AND spool_path <> ''", writeback.StateCompleted).
		Where("remote_verified_at IS NOT NULL AND remote_generation = generation").
		Select("COALESCE(SUM(size), 0)").
		Scan(&completedCacheBytes).Error; err != nil {
		common.ErrorResp(c, err, http.StatusInternalServerError)
		return
	}

	var errorsCount int64
	if err := db.GetDb().
		Model(&model.WebDAVWritebackObject{}).
		Where("last_error <> ''").
		Count(&errorsCount).Error; err != nil {
		common.ErrorResp(c, err, http.StatusInternalServerError)
		return
	}

	common.SuccessResp(c, webDAVWritebackSummary{
		Enabled:                writeback.Enabled(),
		Workers:                conf.Conf.WebDAVWriteback.Workers,
		Receiving:              receiving,
		ReceivingExpectedBytes: receivingExpectedBytes,
		BacklogBytes:             backlogBytes,
		CompletedCacheBytes:      completedCacheBytes,
		MaxPendingSpoolBytes:     conf.Conf.WebDAVWriteback.MaxPendingSpoolMB * uint64(1024*1024),
		ReserveFreeSpaceBytes:    conf.Conf.WebDAVWriteback.ReserveFreeSpaceMB * uint64(1024*1024),
		CompletedCacheTTLMinutes: conf.Conf.WebDAVWriteback.CompletedCacheTTLMinutes,
		Errors:                   errorsCount,
		States:                   states,
		UpdatedAt:                now,
	})
}

func WebDAVWritebackCleanupCompletedCache(c *gin.Context) {
	released, err := writeback.CleanupCompletedCacheNow()
	if err != nil {
		common.ErrorResp(c, err, http.StatusInternalServerError)
		return
	}
	common.SuccessResp(c, webDAVWritebackCleanupResult{Released: released})
}

func webDAVMonitorLimit(c *gin.Context) int {
	limit, err := strconv.Atoi(c.DefaultQuery("limit", strconv.Itoa(webDAVMonitorDefaultLimit)))
	if err != nil || limit <= 0 {
		return webDAVMonitorDefaultLimit
	}
	if limit > webDAVMonitorMaxLimit {
		return webDAVMonitorMaxLimit
	}
	return limit
}

func webDAVMonitorCanonicalState(row *model.WebDAVWritebackObject) string {
	if row.CanonicalState != "" {
		return row.CanonicalState
	}
	if row.State == "deleted" {
		return "deleted"
	}
	return "acked"
}

func WebDAVWritebackMonitorList(c *gin.Context) {
	now := time.Now()
	limit := webDAVMonitorLimit(c)
	state := strings.ToLower(strings.TrimSpace(c.DefaultQuery("state", "active")))
	search := strings.TrimSpace(c.Query("q"))

	rows := make([]webDAVWritebackMonitorRow, 0, limit)
	includeReceiving := state == "active" || state == "all" || state == "receiving"
	includeObjects := state != "receiving"

	if includeReceiving {
		var fences []model.WebDAVWritebackReceiveFence
		query := db.GetDb().
			Model(&model.WebDAVWritebackReceiveFence{}).
			Where("active_receivers > 0 AND receive_lease_until > ?", now)
		if search != "" {
			query = query.Where("path LIKE ?", "%"+search+"%")
		}
		if err := query.Order("updated_at DESC").Limit(limit).Find(&fences).Error; err != nil {
			common.ErrorResp(c, err, http.StatusInternalServerError)
			return
		}
		for _, fence := range fences {
			rows = append(rows, webDAVWritebackMonitorRow{
				ID:              "receive:" + fence.PathKey,
				Path:            fence.Path,
				Name:            fence.Path,
				Size:            fence.LatestExpectedSize,
				ClientState:     "receiving",
				ProviderState:   "not_started",
				ActiveReceivers: fence.ActiveReceivers,
				StartedAt:       fence.LatestStartedAt,
				CreatedAt:       fence.CreatedAt,
				UpdatedAt:       fence.UpdatedAt,
			})
		}
	}

	if includeObjects {
		var objects []model.WebDAVWritebackObject
		query := db.GetDb().
			Model(&model.WebDAVWritebackObject{}).
			Select("id, path, name, is_dir, size, canonical_state, state, generation, remote_generation, payload_sha1, remote_sha1, retry_count, verify_count, last_error, retry_at, remote_verified_at, completed_at, created_at, updated_at")

		switch state {
		case "", "active":
			query = query.Where("state IN ?", []string{writeback.StateQueued, writeback.StateUploading, writeback.StateVerifying})
		case "all":
		case "error":
			query = query.Where("last_error <> ''")
		default:
			query = query.Where("state = ?", state)
		}
		if search != "" {
			query = query.Where("path LIKE ?", "%"+search+"%")
		}
		if err := query.Order("updated_at DESC").Limit(limit).Find(&objects).Error; err != nil {
			common.ErrorResp(c, err, http.StatusInternalServerError)
			return
		}
		for i := range objects {
			row := &objects[i]
			rows = append(rows, webDAVWritebackMonitorRow{
				ID:               strconv.FormatUint(uint64(row.ID), 10),
				Path:             row.Path,
				Name:             row.Name,
				IsDir:            row.IsDir,
				Size:             row.Size,
				ClientState:      webDAVMonitorCanonicalState(row),
				ProviderState:    row.State,
				Generation:       row.Generation,
				RemoteGeneration: row.RemoteGeneration,
				PayloadSHA1:      row.PayloadSHA1,
				RemoteSHA1:       row.RemoteSHA1,
				RetryCount:       row.RetryCount,
				VerifyCount:      row.VerifyCount,
				LastError:        row.LastError,
				RetryAt:          row.RetryAt,
				RemoteVerifiedAt: row.RemoteVerifiedAt,
				CompletedAt:      row.CompletedAt,
				CreatedAt:        row.CreatedAt,
				UpdatedAt:        row.UpdatedAt,
			})
		}
	}

	sort.SliceStable(rows, func(i, j int) bool {
		return rows[i].UpdatedAt.After(rows[j].UpdatedAt)
	})
	if len(rows) > limit {
		rows = rows[:limit]
	}
	common.SuccessResp(c, rows)
}

func WebDAVWritebackMonitorPage(c *gin.Context) {
	c.Header("Cache-Control", "no-store")
	c.Data(http.StatusOK, "text/html; charset=utf-8", []byte(webDAVWritebackMonitorHTML))
}

const webDAVWritebackMonitorHTML = `<!doctype html>
<html lang="en">
<head>
<meta charset="utf-8">
<meta name="viewport" content="width=device-width,initial-scale=1">
<title>WebDAV Writeback Monitor</title>
<style>
:root{font-family:Inter,ui-sans-serif,system-ui,-apple-system,BlinkMacSystemFont,"Segoe UI",sans-serif;color:#172033;background:#f6f8fb}
*{box-sizing:border-box}
body{margin:0}
.wrap{max-width:1500px;margin:0 auto;padding:24px}
.top{display:flex;gap:12px;align-items:center;flex-wrap:wrap;margin-bottom:16px}
h1{font-size:24px;margin:0;margin-right:auto}
button,select,input{border:1px solid #ccd4e0;border-radius:8px;background:#fff;padding:8px 10px;font:inherit}
button{cursor:pointer}
.cards{display:grid;grid-template-columns:repeat(auto-fit,minmax(155px,1fr));gap:10px;margin:14px 0}
.card{background:#fff;border:1px solid #e1e6ef;border-radius:12px;padding:14px}
.card .label{font-size:12px;color:#667085}
.card .value{font-size:24px;font-weight:700;margin-top:3px}
.note{font-size:12px;color:#667085;margin:8px 0 14px}
.status{font-size:13px;margin-left:4px}
.status.error{color:#b42318}
.table-wrap{overflow:auto;background:#fff;border:1px solid #e1e6ef;border-radius:12px}
table{border-collapse:collapse;width:100%;min-width:1250px}
th,td{padding:10px 12px;border-bottom:1px solid #eef1f5;text-align:left;vertical-align:top;font-size:13px}
th{position:sticky;top:0;background:#f8fafc;z-index:1;color:#475467}
.path{max-width:520px;word-break:break-all}
.badge{display:inline-block;border-radius:999px;padding:2px 8px;background:#eef2f6;font-size:12px;white-space:nowrap}
.badge.receiving{background:#e0f2fe;color:#075985}
.badge.queued{background:#fef3c7;color:#92400e}
.badge.uploading{background:#dbeafe;color:#1d4ed8}
.badge.verifying{background:#ede9fe;color:#6d28d9}
.badge.completed,.badge.acked{background:#dcfce7;color:#166534}
.badge.deleted,.badge.error{background:#fee2e2;color:#991b1b}
.err{color:#b42318;max-width:340px;word-break:break-word}
.muted{color:#98a2b3}
.small{font-size:12px}
@media(max-width:700px){.wrap{padding:12px}h1{font-size:20px}}
</style>
</head>
<body>
<div class="wrap">
  <div class="top">
    <h1>WebDAV Writeback Monitor</h1>
    <a id="back" href="#">Back to admin</a>
    <select id="state">
      <option value="active">Active</option>
      <option value="receiving">Receiving</option>
      <option value="queued">Queued</option>
      <option value="uploading">Uploading</option>
      <option value="verifying">Verifying</option>
      <option value="completed">Completed</option>
      <option value="error">Errors</option>
      <option value="all">All</option>
    </select>
    <input id="search" placeholder="Filter path">
    <button id="refresh">Refresh</button>
    <button id="cleanup">Clean verified cache</button>
    <label class="small"><input id="auto" type="checkbox" checked> auto 2s</label>
    <span id="status" class="status"></span>
  </div>
  <div class="cards">
    <div class="card"><div class="label">Receiving</div><div class="value" id="receiving">-</div></div>
    <div class="card"><div class="label">Queued</div><div class="value" id="queued">-</div></div>
    <div class="card"><div class="label">Uploading</div><div class="value" id="uploading">-</div></div>
    <div class="card"><div class="label">Verifying</div><div class="value" id="verifying">-</div></div>
    <div class="card"><div class="label">Completed</div><div class="value" id="completed">-</div></div>
    <div class="card"><div class="label">Errors</div><div class="value" id="errors">-</div></div>
    <div class="card"><div class="label">Durable backlog</div><div class="value" id="backlog">-</div><div class="small muted" id="backlogLimit"></div></div>
    <div class="card"><div class="label">Completed cache</div><div class="value" id="completedCache">-</div><div class="small muted" id="cacheTTL"></div></div>
    <div class="card"><div class="label">Disk reserve</div><div class="value" id="reserve">-</div></div>
    <div class="card"><div class="label">Workers</div><div class="value" id="workers">-</div></div>
  </div>
  <div class="note">Receiving shows active WebDAV request admission. Backlog admission is bounded by Max Pending Spool and the configured free-space reserve; when the local durable buffer is full, OpenList applies WebDAV backpressure instead of accepting data it cannot persist. Clean verified cache removes only completed, current-generation, provider-verified local payload copies; pending/receiving/recovery spools are never removed. On restart, interrupted uploads verify the provider first and continue from the persistent spool or ask Cloud Sync to re-PUT only after local and remote loss are both confirmed.</div>
  <div class="table-wrap">
    <table>
      <thead><tr>
        <th>Path</th><th>Client</th><th>Provider</th><th>Size</th><th>Generation</th>
        <th>Remote generation</th><th>Retry</th><th>Verify</th><th>Updated</th><th>Error</th>
      </tr></thead>
      <tbody id="rows"><tr><td colspan="10" class="muted">Loading…</td></tr></tbody>
    </table>
  </div>
</div>
<script>
(() => {
  const marker="/@manage/webdav-writeback";
  const idx=location.pathname.lastIndexOf(marker);
  const base=idx>=0?location.pathname.slice(0,idx):"";
  const api=base+"/api/admin/webdav-writeback";
  document.getElementById("back").href=base+"/@manage";
  const token=()=>localStorage.getItem("token")||"";
  const status=document.getElementById("status");
  const state=document.getElementById("state");
  const search=document.getElementById("search");
  const rows=document.getElementById("rows");
  const esc=(v)=>String(v??"").replace(/[&<>"']/g,(c)=>({"&":"&amp;","<":"&lt;",">":"&gt;","\"":"&quot;","'":"&#39;"}[c]));
  const fmtBytes=(n)=>{
    n=Number(n||0); if(!n)return "0 B";
    const u=["B","KiB","MiB","GiB","TiB"]; let i=0;
    while(n>=1024&&i<u.length-1){n/=1024;i++}
    return (i===0?n.toFixed(0):n.toFixed(2))+" "+u[i];
  };
  const fmtTime=(v)=>v?new Date(v).toLocaleString():"-";
  const badge=(v)=>'<span class="badge '+esc(v)+'">'+esc(v||"-")+'</span>';
  async function getJSON(url,options={}){
    const headers=Object.assign({Authorization:token()},options.headers||{});
    const r=await fetch(url,Object.assign({},options,{headers}));
    const j=await r.json().catch(()=>({}));
    if(!r.ok||j.code!==200) throw new Error(j.message||("HTTP "+r.status));
    return j.data;
  }
  function stateCount(summary,name){return summary.states?.[name]?.count||0}
  async function refresh(){
    status.className="status";
    status.textContent="Refreshing…";
    try{
      const params=new URLSearchParams({state:state.value,limit:"200"});
      if(search.value.trim())params.set("q",search.value.trim());
      const [summary,list]=await Promise.all([
        getJSON(api+"/summary"),
        getJSON(api+"/list?"+params.toString())
      ]);
      document.getElementById("receiving").textContent=summary.receiving||0;
      document.getElementById("queued").textContent=stateCount(summary,"queued");
      document.getElementById("uploading").textContent=stateCount(summary,"uploading");
      document.getElementById("verifying").textContent=stateCount(summary,"verifying");
      document.getElementById("completed").textContent=stateCount(summary,"completed");
      document.getElementById("errors").textContent=summary.errors||0;
      document.getElementById("backlog").textContent=fmtBytes(summary.backlog_bytes);
      document.getElementById("backlogLimit").textContent=summary.max_pending_spool_bytes?("limit "+fmtBytes(summary.max_pending_spool_bytes)):"unlimited";
      document.getElementById("completedCache").textContent=fmtBytes(summary.completed_cache_bytes);
      document.getElementById("cacheTTL").textContent=summary.completed_cache_ttl_minutes<0?"automatic cleanup disabled":("TTL "+summary.completed_cache_ttl_minutes+" min");
      document.getElementById("reserve").textContent=fmtBytes(summary.reserve_free_space_bytes);
      document.getElementById("workers").textContent=summary.workers||0;
      rows.innerHTML=(list||[]).length?(list||[]).map(x=>
        '<tr>'+
          '<td class="path"><strong>'+esc(x.path)+'</strong><div class="small muted">'+(x.is_dir?"directory":"file")+'</div></td>'+
          '<td>'+badge(x.client_state)+'</td>'+
          '<td>'+badge(x.provider_state)+'</td>'+
          '<td>'+esc(fmtBytes(x.size))+'</td>'+
          '<td>'+esc(x.generation)+'</td>'+
          '<td>'+esc(x.remote_generation)+'</td>'+
          '<td>'+esc(x.retry_count)+'</td>'+
          '<td>'+esc(x.verify_count)+'</td>'+
          '<td class="small">'+esc(fmtTime(x.updated_at))+'</td>'+
          '<td class="err">'+esc(x.last_error||"")+'</td>'+
        '</tr>'
      ).join(""):'<tr><td colspan="10" class="muted">No matching WebDAV writeback entries.</td></tr>';
      status.textContent="Updated "+new Date().toLocaleTimeString();
    }catch(e){
      status.className="status error";
      status.textContent=e.message+(token()?"":" — log in to the OpenList admin UI first");
    }
  }
  document.getElementById("refresh").addEventListener("click",refresh);
  document.getElementById("cleanup").addEventListener("click",async()=>{
    if(!confirm("Remove only completed, provider-verified local WebDAV cache files? Pending uploads will be kept."))return;
    status.className="status";
    status.textContent="Cleaning verified cache…";
    try{
      const result=await getJSON(api+"/cleanup",{method:"POST"});
      status.textContent="Released "+(result?.released||0)+" completed cache entries";
      await refresh();
    }catch(e){
      status.className="status error";
      status.textContent=e.message;
    }
  });
  state.addEventListener("change",refresh);
  search.addEventListener("keydown",(e)=>{if(e.key==="Enter")refresh()});
  setInterval(()=>{if(document.getElementById("auto").checked)refresh()},2000);
  refresh();
})();
</script>
</body>
</html>`
