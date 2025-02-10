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
	"mime"
	"net/http"
	"strconv"
	"strings"
	"unicode"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/regexPattern/fiuba-reviews/scraper_siu"
	"golang.org/x/text/runes"
	"golang.org/x/text/transform"
	"golang.org/x/text/unicode/norm"
)

const BUCKET string = "fiuba-reviews-scraper-siu"

func HandlerScraperSiu(w http.ResponseWriter, r *http.Request) {
	ctx := context.Background()

	slog.SetDefault(slog.New(slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{
		Level: slog.LevelDebug,
	})))

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
		return
	}

	clienteS3 := s3.NewFromConfig(cfg)

	bucket, err := clienteS3.ListObjectsV2(ctx, &s3.ListObjectsV2Input{
		Bucket: aws.String(BUCKET),
	})

	if err != nil {
		slog.Error(err.Error())
		w.WriteHeader(http.StatusInternalServerError)
		return
	}

	carreras := make([]string, 0, len(bucket.Contents))

	rfc2047Dec := new(mime.WordDecoder)

	for _, obj := range bucket.Contents {
		archivo, err := clienteS3.GetObject(ctx, &s3.GetObjectInput{
			Bucket: aws.String(BUCKET),
			Key:    obj.Key,
		})

		if err != nil {
			slog.Error(err.Error())
			continue
		}

		defer archivo.Body.Close()

		meta := archivo.Metadata
		carrera, ok := meta["carrera"]

		slog.Debug(carrera)

		if ok {
			carrera, err = rfc2047Dec.DecodeHeader("=?UTF-8?Q?INGENIER=C3=83=C2=8DA_EN_INFORM=C3=83=C2=81TICA?=")
			if err != nil {
				slog.Error(err.Error())
			}
			slog.Debug(carrera)
			carreras = append(carreras, carrera)
		}
	}

	jsonData, err := json.Marshal(carreras)
	if err != nil {
		slog.Error(err.Error())
		w.WriteHeader(http.StatusInternalServerError)
		return
	}

	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusOK)
	_, err = w.Write(jsonData)
	if err != nil {
		slog.Error(err.Error())
	}
}

func handlerPost(ctx context.Context, w http.ResponseWriter, r *http.Request) {
	cfg, err := config.LoadDefaultConfig(ctx)
	if err != nil {
		slog.Error(err.Error())
		w.WriteHeader(http.StatusInternalServerError)
		return
	}

	clienteS3 := s3.NewFromConfig(cfg)

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

	meta, err := scraper_siu.ObtenerMetaData(string(contenidoSiu))

	slog.Debug(
		"Obtenidos metadatos del contenido del SIU.",
		"carrera", meta.Carrera,
		"cuatrimestre", meta.Cuatri.Numero,
		"anio", meta.Cuatri.Anio,
	)

	if err != nil {
		w.WriteHeader(http.StatusBadRequest)
		_, err := w.Write([]byte(err.Error()))

		if err != nil {
			slog.Error(err.Error())
		}

		return
	}

	materias := scraper_siu.ObtenerMaterias(meta.Cuatri.Contenido)
	objBody, err := json.Marshal(materias)

	if err != nil {
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
	carrera = base64.StdEncoding.EncodeToString([]byte(meta.Carrera))

	slog.Info(fmt.Sprintf("Encodeado nombre de la carrera en base64: '%v'.", carrera))

	_, err = clienteS3.PutObject(ctx, &s3.PutObjectInput{
		Bucket:          aws.String(BUCKET),
		Key:             aws.String(objKey),
		ContentType:     aws.String("application/json"),
		ContentLanguage: aws.String("es"),
		Metadata: map[string]string{
			"carrera":      carrera,
			"cuatrimestre": strconv.Itoa(meta.Cuatri.Numero),
			"anio":         strconv.Itoa(meta.Cuatri.Anio),
		},
		Body: bytes.NewReader(objBody),
	})

	if err != nil {
		slog.Error(err.Error())

		w.WriteHeader(http.StatusInternalServerError)
		_, err := w.Write([]byte("Error interno almacenando la información."))

		if err != nil {
			slog.Error(err.Error())
		}

		return
	}

	slog.Info(fmt.Sprintf("Escrito archivo '%v' con éxito.", objKey))

	w.WriteHeader(http.StatusCreated)
}
