package api

import (
	"bytes"
	"context"
	"os"

	"encoding/base64"
	"encoding/json"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"strconv"
	"strings"
	"unicode"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/regexPattern/fiuba-reviews/scraper"
	"golang.org/x/text/runes"
	"golang.org/x/text/transform"
	"golang.org/x/text/unicode/norm"
)

const BUCKET string = "fiuba-reviews-siu"

type plan struct {
	Carrera string `json:"carrera"`
	Cuatri  cuatri `json:"cuatri"`
}

type cuatri struct {
	Numero int `json:"numero"`
	Anio   int `json:"anio"`
}

func init() {
	slog.SetDefault(slog.New(slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{
		AddSource: true,
		Level:     slog.LevelInfo,
	})))
}

func HandlerScraperSiu(w http.ResponseWriter, r *http.Request) {
	ctx := context.Background()

	switch r.Method {
	case "GET":
		handlerGet(ctx, w)
	case "POST":
		handlerPost(ctx, w, r)
	default:
		w.WriteHeader(http.StatusMethodNotAllowed)
	}
}

func handlerGet(ctx context.Context, w http.ResponseWriter) {
	cfg, err := config.LoadDefaultConfig(ctx)
	if err != nil {
		slog.Error(err.Error())

		w.WriteHeader(http.StatusInternalServerError)

		_, err := w.Write([]byte("Error interno conectando con la base de datos."))
		if err != nil {
			slog.Error(err.Error())
		}

		return
	}

	client := s3.NewFromConfig(cfg)

	bucket, err := client.ListObjectsV2(ctx, &s3.ListObjectsV2Input{
		Bucket: aws.String(BUCKET),
	})

	if err != nil {
		slog.Error(err.Error())

		w.WriteHeader(http.StatusInternalServerError)

		_, err := w.Write([]byte("Error interno obteniendo listado de la base de datos."))
		if err != nil {
			slog.Error(err.Error())
		}

		return
	}

	planes := make([]*plan, 0, len(bucket.Contents))

	for _, obj := range bucket.Contents {
		objLogger := slog.Default().With("objKey", obj.Key)

		objHead, err := client.HeadObject(ctx, &s3.HeadObjectInput{
			Bucket: aws.String(BUCKET),
			Key:    obj.Key,
		})

		if err != nil {
			objLogger.Error(err.Error())

			w.WriteHeader(http.StatusInternalServerError)

			_, err := w.Write([]byte("Error interno al obtener planes existentes."))
			if err != nil {
				slog.Error(err.Error())
			}

			return
		}

		plan, err := parsearMetaDataPlan(objHead)
		if err != nil {
			objLogger.Error(err.Error())

			w.WriteHeader(http.StatusInternalServerError)

			_, err := w.Write([]byte(err.Error()))
			if err != nil {
				slog.Error(err.Error())
			}

			return
		}

		planes = append(planes, plan)
	}

	planesJson, err := json.Marshal(planes)
	if err != nil {
		slog.Error(err.Error())

		w.WriteHeader(http.StatusInternalServerError)

		_, err = w.Write([]byte("Error interno serializando respuesta."))
		if err != nil {
			slog.Error(err.Error())
		}

		return
	}

	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusOK)

	_, err = w.Write(planesJson)
	if err != nil {
		slog.Error(err.Error())
	}
}

func handlerPost(ctx context.Context, w http.ResponseWriter, r *http.Request) {
	cfg, err := config.LoadDefaultConfig(ctx)
	if err != nil {
		slog.Error(err.Error())

		w.WriteHeader(http.StatusInternalServerError)

		_, err := w.Write([]byte("Error interno conectando con la base de datos."))
		if err != nil {
			slog.Error(err.Error())
		}

		return
	}

	client := s3.NewFromConfig(cfg)

	defer r.Body.Close()

	contenidoSiu, err := io.ReadAll(r.Body)

	if err != nil {
		slog.Error(err.Error())

		w.WriteHeader(http.StatusInternalServerError)

		_, err := w.Write([]byte("Error interno leyendo el contenido de la solicitud."))
		if err != nil {
			slog.Error(err.Error())
		}

		return
	}

	meta, err := scraper.ObtenerMetaData(string(contenidoSiu))

	if err != nil {
		w.WriteHeader(http.StatusBadRequest)
		_, err := w.Write([]byte(err.Error()))

		if err != nil {
			slog.Error(err.Error())
		}

		return
	}

	materias := scraper.ObtenerMaterias(meta.Cuatri.Contenido)
	objBody, err := json.Marshal(materias)

	if err != nil {
		slog.Error(err.Error())

		w.WriteHeader(http.StatusInternalServerError)
		_, err := w.Write([]byte("Error interno serializando información scrapeada."))

		if err != nil {
			slog.Error(err.Error())
		}

		return
	}

	// Se eliminan los diacríticos para generar el file path (object key).
	t := transform.Chain(norm.NFD, runes.Remove(runes.In(unicode.Mn)), norm.NFC)
	carrera, _, _ := transform.String(t, meta.Carrera)
	carrera = strings.ToLower(strings.ReplaceAll(carrera, " ", "-"))

	objKey := fmt.Sprintf("%v-%vC-%v.json", carrera, meta.Cuatri.Numero, meta.Cuatri.Anio)

	// Se le hace encoding al valor del key de metadata 'carrera'. Esto debido a
	// que AWS require este encoding cuando los valores de los headers (los
	// metadatos son headers) contiene caracters no US-ASCII, como es el caso de
	// la mayoría de los nombres de las carreras de la facultad.
	//
	// Realmente la SDK de S3 se encarga automáticamente de esto, pero por alguna
	// razón me está arrojando un encoding erróneo de las tildes, así que
	// prefiero hacerlo manual.
	//
	// Más información: https://docs.aws.amazon.com/AmazonS3/latest/userguide/UsingMetadata.html#UserMetadata
	carreraB64 := base64.StdEncoding.EncodeToString([]byte(meta.Carrera))

	obj := &s3.PutObjectInput{
		Bucket:          aws.String(BUCKET),
		Key:             aws.String(objKey),
		ContentType:     aws.String("application/json"),
		ContentLanguage: aws.String("es"),
		Metadata: map[string]string{
			"carrera":       carreraB64,
			"cuatri-numero": strconv.Itoa(meta.Cuatri.Numero),
			"cuatri-anio":   strconv.Itoa(meta.Cuatri.Anio),
		},
		Body: bytes.NewReader(objBody),
	}

	if yaExiste, err := planYaExiste(ctx, client, obj); err != nil {
		w.WriteHeader(http.StatusInternalServerError)
		return
	} else if yaExiste {
		w.WriteHeader(http.StatusAccepted)
		return
	}

	_, err = client.PutObject(ctx, obj)

	if err != nil {
		slog.Error(err.Error())

		w.WriteHeader(http.StatusInternalServerError)
		_, err := w.Write([]byte("Error interno almacenando la información."))

		if err != nil {
			slog.Error(err.Error())
		}

		return
	}

	w.WriteHeader(http.StatusCreated)

	slog.Info(fmt.Sprintf("Escrito archivo '%v' con éxito.", objKey))
}

func parsearMetaDataPlan(objHead *s3.HeadObjectOutput) (*plan, error) {
	meta := objHead.Metadata

	carreraB64, okCarrera := meta["carrera"]
	numeroStr, okNum := meta["cuatri-numero"]
	anioStr, okAnio := meta["cuatri-anio"]

	var err error

	if !okCarrera {
		err = fmt.Errorf("Metadato 'carrera' no encontrado.")
	} else if !okNum {
		err = fmt.Errorf("Metadato 'cuatri-numero' no encontrado.")
	} else if !okAnio {
		err = fmt.Errorf("Metadato 'cuatri-anio' no encontrado.")
	}

	if err != nil {
		return nil, err
	}

	carrera, errCarrera := base64.StdEncoding.DecodeString(carreraB64)
	numero, errNum := strconv.Atoi(numeroStr)
	anio, errAnio := strconv.Atoi(anioStr)

	if errCarrera != nil {
		err = fmt.Errorf("Error al deserializar 'carrera' como string.")
	} else if errNum != nil {
		err = fmt.Errorf("Error al deserializar 'cuatri-numero' como entero.")
	} else if errAnio != nil {
		err = fmt.Errorf("Error al deserializar 'cuatri-anio' como entero.")
	}

	if err != nil {
		return nil, err
	}

	plan := &plan{
		Carrera: string(carrera),
		Cuatri: cuatri{
			Numero: numero,
			Anio:   anio,
		},
	}

	return plan, nil
}

func planYaExiste(ctx context.Context, client *s3.Client, newObj *s3.PutObjectInput) (bool, error) {
	bucket, err := client.ListObjectsV2(ctx, &s3.ListObjectsV2Input{
		Bucket: aws.String(BUCKET),
	})

	if err != nil {
		slog.Error(err.Error())
		return false, fmt.Errorf("Error interno al comparar con planes ya existentes.")
	}

	for _, existObj := range bucket.Contents {
		existObjHead, err := client.HeadObject(ctx, &s3.HeadObjectInput{
			Bucket: aws.String(BUCKET),
			Key:    existObj.Key,
		})

		if err != nil {
			slog.Error(err.Error(), "objKey", existObj.Key)
			return false, fmt.Errorf("Error interno al comparar con planes ya existentes.")
		}

		meta := existObjHead.Metadata

		carrera, okCarrera := meta["carrera"]
		numero, okNum := meta["cuatri-numero"]
		anio, okAnio := meta["cuatri-anio"]

		if okCarrera && okNum && okAnio {
			if newObj.Metadata["carrera"] == carrera &&
				newObj.Metadata["cuatri-numero"] == numero &&
				newObj.Metadata["cuatri-anio"] == anio {

				slog.Info(
					"Cache hit de plan.",
					"objKey", existObj.Key,
				)

				return true, nil
			}
		}
	}

	return false, nil
}
