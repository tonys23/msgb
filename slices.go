package msgb

func IndexOf[T comparable](s []T, e T) int {
	for i, v := range s {
		if v == e {
			return i
		}
	}
	return -1
}

func Group[
	T interface{},
	R comparable](
	s []T,
	gfn func(T) R) [][]T {

	r := [][]T{}
	gps := []R{}
	for _, v := range s {
		g := gfn(v)
		if IndexOf(gps, g) == -1 {
			gps = append(gps, g)
			rit := []T{}
			for _, it := range s {
				if gfn(it) == g {
					rit = append(rit, it)
				}
			}
			r = append(r, rit)
		}
	}
	return r
}
