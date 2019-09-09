package databases

// PanicIfErr panics when passed a non-nil error
// TODO: Remove the need for this by continuing to bubble up errors
func PanicIfErr(err error) {
	if err != nil {
		panic(err.Error())
	}
}
