package main
import(
	"github.com/nahid/gohttp"
	"fmt"
)


func main() {
	req := gohttp.NewRequest()
	ch := make(chan *gohttp.AsyncResponse)

	for i := 0; i <= 100; i++ {
		req.AsyncGet("http://127.0.0.1:8000/echo", ch)
	}
	for i:=0; i<=100; i++ {
		op := <-ch

		fmt.Println(op.GetBodyAsString())
	}
}