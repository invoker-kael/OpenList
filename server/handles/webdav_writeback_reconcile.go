package handles

import (
	"errors"
	"net/http"
	"strconv"
	"strings"

	"github.com/OpenListTeam/OpenList/v4/internal/writeback"
	"github.com/OpenListTeam/OpenList/v4/server/common"
	"github.com/gin-gonic/gin"
	"gorm.io/gorm"
)

type webDAVReconciliationAction struct {
	RunID              uint  `json:"run_id"`
	ExpectedCandidates int64 `json:"expected_candidates"`
}

type webDAVReconciliationCandidates struct {
	Items []writeback.ReconciliationCandidate `json:"items"`
	Total int64                              `json:"total"`
	Page  int                                `json:"page"`
	Limit int                                `json:"page_size"`
}

func WebDAVWritebackReconciliation(c *gin.Context) {
	run, err := writeback.CurrentReconciliation()
	if err != nil {
		common.ErrorResp(c, err, http.StatusInternalServerError)
		return
	}
	var candidates int64
	canCancel := false
	if run == nil {
		run, err = writeback.LatestReconciliation()
	} else {
		candidates, err = writeback.ReconciliationCandidateCount()
		if err == nil {
			canCancel, err = writeback.ReconciliationCanCancel(run.ID)
		}
	}
	if err != nil {
		common.ErrorResp(c, err, http.StatusInternalServerError)
		return
	}
	common.SuccessResp(c, gin.H{"run": run, "candidate_count": candidates, "can_cancel": canCancel})
}

func WebDAVWritebackReconciliationCandidates(c *gin.Context) {
	limit, err := strconv.Atoi(c.DefaultQuery("limit", "100"))
	if err != nil || limit < 1 || limit > 200 {
		limit = 100
	}
	page, err := strconv.Atoi(c.DefaultQuery("page", "1"))
	if err != nil || page < 1 {
		page = 1
	}
	items, err := writeback.ReconciliationCandidates(page, limit)
	if err != nil {
		common.ErrorResp(c, err, http.StatusInternalServerError)
		return
	}
	total, err := writeback.ReconciliationCandidateCount()
	if err != nil {
		common.ErrorResp(c, err, http.StatusInternalServerError)
		return
	}
	common.SuccessResp(c, webDAVReconciliationCandidates{Items: items, Total: total, Page: page, Limit: limit})
}

func WebDAVWritebackReconciliationStart(c *gin.Context) {
	var req struct {
		Root string `json:"root"`
	}
	if err := c.ShouldBindJSON(&req); err != nil {
		common.ErrorResp(c, err, http.StatusBadRequest)
		return
	}
	run, err := writeback.StartReconciliation(strings.TrimSpace(req.Root))
	if err != nil {
		common.ErrorResp(c, err, http.StatusConflict)
		return
	}
	common.SuccessResp(c, gin.H{"run": run})
}

func WebDAVWritebackReconciliationFinishCollection(c *gin.Context) {
	var req webDAVReconciliationAction
	if err := c.ShouldBindJSON(&req); err != nil {
		common.ErrorResp(c, err, http.StatusBadRequest)
		return
	}
	started, err := writeback.FinishReconciliationCollection(req.RunID)
	if err != nil {
		common.ErrorResp(c, err, http.StatusConflict)
		return
	}
	common.SuccessResp(c, gin.H{"started": started})
}


func WebDAVWritebackReconciliationStartComparison(c *gin.Context) {
	var req webDAVReconciliationAction
	if err := c.ShouldBindJSON(&req); err != nil {
		common.ErrorResp(c, err, http.StatusBadRequest)
		return
	}
	started, err := writeback.StartReconciliationComparison(req.RunID)
	if err != nil {
		common.ErrorResp(c, err, http.StatusConflict)
		return
	}
	common.SuccessResp(c, gin.H{"started": started})
}
func WebDAVWritebackReconciliationDeleteExtras(c *gin.Context) {
	var req webDAVReconciliationAction
	if err := c.ShouldBindJSON(&req); err != nil {
		common.ErrorResp(c, err, http.StatusBadRequest)
		return
	}
	started, err := writeback.ApplyReconciliationDeletes(req.RunID, req.ExpectedCandidates)
	if err != nil {
		common.ErrorResp(c, err, http.StatusConflict)
		return
	}
	common.SuccessResp(c, gin.H{"started": started})
}

func WebDAVWritebackReconciliationFinish(c *gin.Context) {
	var req webDAVReconciliationAction
	if err := c.ShouldBindJSON(&req); err != nil {
		common.ErrorResp(c, err, http.StatusBadRequest)
		return
	}
	if err := writeback.CloseReconciliation(req.RunID); err != nil {
		common.ErrorResp(c, err, http.StatusConflict)
		return
	}
	common.SuccessResp(c, gin.H{"finished": true})
}

func WebDAVWritebackReconciliationCancel(c *gin.Context) {
	var req webDAVReconciliationAction
	if err := c.ShouldBindJSON(&req); err != nil {
		common.ErrorResp(c, err, http.StatusBadRequest)
		return
	}
	if err := writeback.CancelReconciliation(req.RunID); err != nil {
		if errors.Is(err, gorm.ErrRecordNotFound) {
			common.ErrorResp(c, err, http.StatusNotFound)
			return
		}
		common.ErrorResp(c, err, http.StatusConflict)
		return
	}
	common.SuccessResp(c, gin.H{"canceled": true})
}
