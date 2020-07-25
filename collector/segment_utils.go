package collector

import "strings"

// 转换参考：http://gpdb.docs.pivotal.io/530/ref_guide/system_catalogs/gp_segment_configuration.html
var (
	sgStatus = map[string]float64{"u": 1, "d": 0}                 //1-UP; 0-DOWN
	sgMode   = map[string]float64{"s": 1, "r": 2, "c": 3, "n": 4} // 1-synchronized ; 2-resyncing; 3-change logging; 4-not synchronized
	sgRole   = map[string]float64{"p": 1, "m": 2}                 //1-Primary ; 2-Mirror
)

func getRole(role string) float64 {
	lowerR := strings.ToLower(role)

	if rf, ok := sgRole[lowerR]; ok {
		return rf
	}

	return 2
}

func getMode(mode string) float64 {
	lowerM := strings.ToLower(mode)

	if mf, ok := sgMode[lowerM]; ok {
		return mf
	}

	return 4
}

func getStatus(status string) float64 {
	lowerS := strings.ToLower(status)

	if sf, ok := sgStatus[lowerS]; ok {
		return sf
	}

	return 0
}
