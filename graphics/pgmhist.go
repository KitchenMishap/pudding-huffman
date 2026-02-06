package graphics

import (
	"fmt"
	"os"
)

const width = 1920 * 2
const height = 1080 * 2

func TestPgmHist() {
	pgm := PgmHist{}
	pgm.PlotPoint(0.75, 0.75)
	pgm.PlotVertical(0.5)
	pgm.PlotHorizontal(0.5)
	pgm.Output("PgmTest.pgm")
}

type PgmHist struct {
	data [width][height]uint16
}

func (ph *PgmHist) PlotPoint(x float64, y float64) {
	pixelX := int(width * x)
	pixelY := int(height * (1 - y))
	ph.PlotPixel(pixelX, pixelY)
	ph.PlotPixel(pixelX, pixelY)
	ph.PlotPixel(pixelX-1, pixelY)
	ph.PlotPixel(pixelX+1, pixelY)
	ph.PlotPixel(pixelX, pixelY+1)
	ph.PlotPixel(pixelX, pixelY-1)
}

func (ph *PgmHist) PlotVertical(x float64) {
	pixelX := int(width * x)
	for y := range height {
		ph.PlotPixel(pixelX, y)
	}
}

func (ph *PgmHist) PlotHorizontal(y float64) {
	pixelY := int(height * y)
	for x := range width {
		ph.PlotPixel(x, pixelY)
	}
}

func (ph *PgmHist) PlotPixel(x int, y int) {
	if x >= 0 && x < width && y >= 0 && y < height {
		ph.data[x][y]++
	}
}

func (ph *PgmHist) Output(filename string) {
	fp, _ := os.Create(filename)
	fmt.Printf("Writing pgm file")
	fmt.Fprintf(fp, "P5 %d %d 65535\n", width, height)
	for y := 0; y < height; y++ {
		for x := 0; x < width; x++ {
			lsb := byte(ph.data[x][y] & 0xFF)
			msb := byte((ph.data[x][y] & 0xFF00) >> 8)
			fp.Write([]byte{msb, lsb})
		}
	}
	fp.Close()
	fmt.Printf("File written")
}
