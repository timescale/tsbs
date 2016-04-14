package main

import "math/rand"

type Distribution interface {
	Next(i int64) float64
}

type NormalDistribution struct {
	Mean   float64
	StdDev float64
}

func (d *NormalDistribution) Next(i int64) float64 {
	return rand.NormFloat64()*d.StdDev + d.Mean
}

type UniformDistribution struct {
	Low  float64
	High float64
}

func (d *UniformDistribution) Next(i int64) float64 {
	x := rand.Float64() // uniform
	x *= d.High - d.Low
	x += d.Low
	return x
}

type RandomWalkDistribution struct {
	State float64
	Step  Distribution
}

func (d *RandomWalkDistribution) Next(i int64) float64 {
	d.State += d.Step.Next(i)
	return d.State
}
