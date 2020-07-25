package collector

import "errors"

/**
* 函数：combineErr
* 功能：error的组合
 */
func combineErr(errs ...error) error {
	var errStr string
	for _, err := range errs {
		if err != nil {
			if errStr == "" {
				errStr += err.Error()
			} else {
				errStr += "; " + err.Error()
			}

		}
	}

	if errStr == "" {
		return nil
	} else {
		return errors.New(errStr)
	}
}
