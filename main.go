package main

import eimerkette "github.com/mabels/eimer-kette/cmd/eimer-kette"

func main() {
	eimerkette.PulumiMain()
	eimerkette.LambdaMain()
	eimerkette.CliMain()
}
