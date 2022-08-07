package training

import (
	"bufio"
	"errors"
	"io"
	"os"
	"path/filepath"
	"strings"

	"d7y.io/dragonfly/v2/scheduler/training/models"

	"github.com/sjwhitworth/golearn/base"
)

// Train provides training functions.
type Train struct {
	BaseDir  string
	FileName string
	// currentRecordLine capacity of lines which training has.
	CurrentRecordLine int
	Options           *TrainOptions
}

// New return a Training instance.
func New(baseDir string, fileName string, option ...TrainOptionFunc) (*Train, error) {
	t := &Train{
		BaseDir:  baseDir,
		FileName: filepath.Join(baseDir, fileName),
		Options:  &TrainOptions{},
	}
	for _, o := range option {
		o(t.Options)
	}

	file, err := os.Open(t.FileName)
	if err != nil {
		return nil, err
	}

	// TODO error handle
	file.Close()

	return t, nil
}

// LoadRecord read record from file and transform it to instance.
func (t *Train) LoadRecord(reader *bufio.Reader) (*base.DenseInstances, error) {
	if t.CurrentRecordLine < t.Options.MaxRecordLine {
		buffer := ""
		for i := 0; i < t.Options.MaxBufferLine; i++ {
			readString, err := reader.ReadString('\n')
			if err != nil && err != io.EOF {
				return nil, err
			}
			if len(readString) == 0 {
				break
			}
			buffer += readString
			t.CurrentRecordLine += 1
		}
		if buffer == "" {
			return nil, errors.New("file empty")
		}
		strReader := strings.NewReader(buffer)
		instance, err := base.ParseCSVToInstancesFromReader(strReader, false)
		if err != nil {
			return nil, err
		}
		return instance, nil
	}
	return nil, nil
}

// PreProcess load and clean data before training.
func (t *Train) PreProcess() (map[float64]*base.DenseInstances, error) {
	f, _ := os.Open(t.FileName)
	result := make(map[float64]*base.DenseInstances, 0)
	reader := bufio.NewReader(f)
	for {
		instance, err := t.LoadRecord(reader)
		if err != nil {
			if err.Error() == "file empty" {
				break
			}
			return nil, err
		}
		effectiveArr, err := MissingValue(instance)
		if err != nil {
			return nil, err
		}
		err = Normalize(instance, effectiveArr, false)
		if err != nil {
			return nil, err
		}
		err = Split(instance, effectiveArr, result)
		if err != nil {
			return nil, err
		}
	}
	// TODO error check
	f.Close()
	return result, nil
}

// TrainProcess fit and evaluate models for each parent peer.
func (t *Train) TrainProcess(trainMap map[float64]*base.DenseInstances) (map[float64]*LinearModel, error) {
	result := make(map[float64]*LinearModel)
	for key, value := range trainMap {
		model := models.NewLinearRegression()
		// TODO percent should also transfer to config
		train, test := base.InstancesTrainTestSplit(value, TestSetPercent)
		err := model.Fit(train, t.Options.LearningRate)
		if err != nil {
			return nil, err
		}
		out, err := model.Predict(test)
		if err != nil {
			return nil, err
		}

		evaluate, err := Evaluate(out, test)
		if err != nil {
			return nil, err
		}
		err = evaluate.CheckEval()
		if err != nil {
			return nil, err
		}

		result[key] = &LinearModel{
			Model: model,
			Ev:    evaluate,
		}
	}
	return result, nil
}
