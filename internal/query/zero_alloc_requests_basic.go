package query

import (
	"errors"

	"github.com/23skdu/longbow/internal/core"
)

func ParseDatasetRequest(data []byte, dsName *string) error {
	i := SkipWhitespace(data, 0)
	if i >= len(data) || data[i] != '{' {
		return errors.New("expected opening brace")
	}
	i++

	for i < len(data) {
		i = SkipWhitespace(data, i)
		if i >= len(data) {
			return errors.New("unexpected end of JSON")
		}
		if data[i] == '}' {
			return nil
		}
		key, newPos, err := ParseString(data, i)
		if err != nil {
			return err
		}
		i = newPos
		i = SkipWhitespace(data, i)
		if i >= len(data) || data[i] != ':' {
			return errors.New("expected colon")
		}
		i++
		i = SkipWhitespace(data, i)

		if key == "dataset" || key == "name" || key == "namespace" {
			val, newPos, err := ParseString(data, i)
			if err != nil {
				return err
			}
			*dsName = val
			i = newPos
		} else {
			i, err = SkipValue(data, i)
			if err != nil {
				return err
			}
		}

		i = SkipWhitespace(data, i)
		if i < len(data) && data[i] == ',' {
			i++
		}
	}
	return nil
}

func ParseSearchByIDRequest(data []byte, req *core.VectorSearchByIDRequest) error {
	i := SkipWhitespace(data, 0)
	if i >= len(data) || data[i] != '{' {
		return errors.New("expected opening brace")
	}
	i++

	for i < len(data) {
		i = SkipWhitespace(data, i)
		if i >= len(data) {
			return errors.New("unexpected end of JSON")
		}
		if data[i] == '}' {
			return nil
		}
		if data[i] != '"' {
			return errors.New("expected quote for key")
		}
		key, newPos, err := ParseString(data, i)
		if err != nil {
			return err
		}
		i = newPos
		i = SkipWhitespace(data, i)
		if i >= len(data) || data[i] != ':' {
			return errors.New("expected colon")
		}
		i++
		i = SkipWhitespace(data, i)

		switch key {
		case "dataset":
			val, newPos, err := ParseString(data, i)
			if err != nil {
				return err
			}
			req.Dataset = val
			i = newPos
		case "id":
			val, newPos, err := ParseString(data, i)
			if err != nil {
				return err
			}
			req.ID = val
			i = newPos
		case "k":
			val, newPos, err := ParseInt64(data, i)
			if err != nil {
				return err
			}
			req.K = int(val)
			i = newPos
		case "include_vectors":
			val, newPos, err := ParseBool(data, i)
			if err != nil {
				return err
			}
			req.IncludeVectors = val
			i = newPos
		case "vector_format":
			val, newPos, err := ParseString(data, i)
			if err != nil {
				return err
			}
			req.VectorFormat = val
			i = newPos
		case "vector_type":
			val, newPos, err := ParseString(data, i)
			if err != nil {
				return err
			}
			req.VectorType = val
			i = newPos
		case "turboquant_bits":
			val, newPos, err := ParseInt64(data, i)
			if err != nil {
				return err
			}
			req.TurboQuantBits = int(val)
			i = newPos
		default:
			i, err = SkipValue(data, i)
			if err != nil {
				return err
			}
		}

		i = SkipWhitespace(data, i)
		if i < len(data) && data[i] == ',' {
			i++
		}
	}
	return nil
}

func ParseRecommendRequest(data []byte, req *core.RecommendRequest) error {
	i := SkipWhitespace(data, 0)
	if i >= len(data) || data[i] != '{' {
		return errors.New("expected opening brace")
	}
	i++

	for i < len(data) {
		i = SkipWhitespace(data, i)
		if i >= len(data) {
			return errors.New("unexpected end of JSON")
		}
		if data[i] == '}' {
			return nil
		}
		key, newPos, err := ParseString(data, i)
		if err != nil {
			return err
		}
		i = newPos
		i = SkipWhitespace(data, i)
		if i >= len(data) || data[i] != ':' {
			return errors.New("expected colon")
		}
		i++
		i = SkipWhitespace(data, i)

		switch key {
		case "dataset":
			val, newPos, err := ParseString(data, i)
			if err != nil {
				return err
			}
			req.Dataset = val
			i = newPos
		case "seed_ids":
			if i < len(data) && data[i] == '[' {
				i++
				for i < len(data) {
					i = SkipWhitespace(data, i)
					if data[i] == ']' {
						i++
						break
					}
					val, newPos, err := ParseString(data, i)
					if err != nil {
						return err
					}
					req.SeedIDs = append(req.SeedIDs, val)
					i = newPos
					i = SkipWhitespace(data, i)
					if i < len(data) && data[i] == ',' {
						i++
					}
				}
			} else {
				i, _ = SkipValue(data, i)
			}
		case "k":
			val, newPos, err := ParseInt64(data, i)
			if err != nil {
				return err
			}
			req.K = int(val)
			i = newPos
		case "alpha":
			val, newPos, err := ParseFloat32(data, i)
			if err != nil {
				return err
			}
			req.Alpha = val
			i = newPos
		case "max_hops":
			val, newPos, err := ParseInt64(data, i)
			if err != nil {
				return err
			}
			req.MaxHops = int(val)
			i = newPos
		case "decay":
			val, newPos, err := ParseFloat32(data, i)
			if err != nil {
				return err
			}
			req.Decay = val
			i = newPos
		default:
			i, err = SkipValue(data, i)
			if err != nil {
				return err
			}
		}

		i = SkipWhitespace(data, i)
		if i < len(data) && data[i] == ',' {
			i++
		}
	}
	return nil
}

func ParseGeoSearchRequest(data []byte, req *core.GeoSearchRequest) error {
	i := SkipWhitespace(data, 0)
	if i >= len(data) || data[i] != '{' {
		return errors.New("expected opening brace")
	}
	i++

	for i < len(data) {
		i = SkipWhitespace(data, i)
		if i >= len(data) {
			return errors.New("unexpected end of JSON")
		}
		if data[i] == '}' {
			return nil
		}
		key, newPos, err := ParseString(data, i)
		if err != nil {
			return err
		}
		i = newPos
		i = SkipWhitespace(data, i)
		if i >= len(data) || data[i] != ':' {
			return errors.New("expected colon")
		}
		i++
		i = SkipWhitespace(data, i)

		switch key {
		case "dataset":
			val, newPos, err := ParseString(data, i)
			if err != nil {
				return err
			}
			req.Dataset = val
			i = newPos
		case "center":
			newPos, err := ParseGeoPoint(data, i, &req.Center)
			if err != nil {
				return err
			}
			i = newPos
		case "radius_km":
			val, newPos, err := ParseFloat32(data, i)
			if err != nil {
				return err
			}
			req.RadiusKm = float64(val)
			i = newPos
		case "box":
			if req.Box == nil {
				req.Box = &core.GeoBoundingBox{}
			}
			newPos, err := ParseGeoBoundingBox(data, i, req.Box)
			if err != nil {
				return err
			}
			i = newPos
		case "k":
			val, newPos, err := ParseInt64(data, i)
			if err != nil {
				return err
			}
			req.K = int(val)
			i = newPos
		case "search_type":
			val, newPos, err := ParseString(data, i)
			if err != nil {
				return err
			}
			req.SearchType = val
			i = newPos
		case "query_vector", "vector":
			vec, newPos, err := parseFloat32Slice(data, i)
			if err != nil {
				return err
			}
			req.QueryVector = vec
			i = newPos
		case "filters":
			newPos, err := ParseFilterArray(data, i, &req.Filters)
			if err != nil {
				return err
			}
			i = newPos
		default:
			i, err = SkipValue(data, i)
			if err != nil {
				return err
			}
		}

		i = SkipWhitespace(data, i)
		if i < len(data) && data[i] == ',' {
			i++
		}
	}
	return nil
}

func ParseGeoPoint(data []byte, pos int, point *core.GeoPoint) (int, error) {
	pos = SkipWhitespace(data, pos)
	if pos >= len(data) || data[pos] != '{' {
		return pos, errors.New("expected { for geopoint")
	}
	pos++
	for pos < len(data) {
		pos = SkipWhitespace(data, pos)
		if data[pos] == '}' {
			return pos + 1, nil
		}
		key, newPos, err := ParseString(data, pos)
		if err != nil {
			return pos, err
		}
		pos = newPos
		pos = SkipWhitespace(data, pos)
		if data[pos] != ':' {
			return pos, errors.New("expected colon")
		}
		pos++
		pos = SkipWhitespace(data, pos)
		switch key {
		case "lat":
			val, newPos, err := ParseFloat32(data, pos)
			if err != nil {
				return pos, err
			}
			point.Lat = float64(val)
			pos = newPos
		case "lon":
			val, newPos, err := ParseFloat32(data, pos)
			if err != nil {
				return pos, err
			}
			point.Lon = float64(val)
			pos = newPos
		case "name":
			val, newPos, err := ParseString(data, pos)
			if err != nil {
				return pos, err
			}
			point.Name = val
			pos = newPos
		default:
			pos, err = SkipValue(data, pos)
			if err != nil {
				return pos, err
			}
		}
		pos = SkipWhitespace(data, pos)
		if pos < len(data) && data[pos] == ',' {
			pos++
		}
	}
	return pos, nil
}

func ParseGeoBoundingBox(data []byte, pos int, box *core.GeoBoundingBox) (int, error) {
	pos = SkipWhitespace(data, pos)
	if pos >= len(data) || data[pos] != '{' {
		return pos, errors.New("expected { for geobox")
	}
	pos++
	for pos < len(data) {
		pos = SkipWhitespace(data, pos)
		if data[pos] == '}' {
			return pos + 1, nil
		}
		key, newPos, err := ParseString(data, pos)
		if err != nil {
			return pos, err
		}
		pos = newPos
		pos = SkipWhitespace(data, pos)
		if data[pos] != ':' {
			return pos, errors.New("expected colon")
		}
		pos++
		pos = SkipWhitespace(data, pos)
		switch key {
		case "min_lat":
			val, newPos, err := ParseFloat32(data, pos)
			if err != nil {
				return pos, err
			}
			box.MinLat = float64(val)
			pos = newPos
		case "max_lat":
			val, newPos, err := ParseFloat32(data, pos)
			if err != nil {
				return pos, err
			}
			box.MaxLat = float64(val)
			pos = newPos
		case "min_lon":
			val, newPos, err := ParseFloat32(data, pos)
			if err != nil {
				return pos, err
			}
			box.MinLon = float64(val)
			pos = newPos
		case "max_lon":
			val, newPos, err := ParseFloat32(data, pos)
			if err != nil {
				return pos, err
			}
			box.MaxLon = float64(val)
			pos = newPos
		default:
			pos, err = SkipValue(data, pos)
			if err != nil {
				return pos, err
			}
		}
		pos = SkipWhitespace(data, pos)
		if pos < len(data) && data[pos] == ',' {
			pos++
		}
	}
	return pos, nil
}
