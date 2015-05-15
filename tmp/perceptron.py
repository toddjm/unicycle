#!/usr/bin/env python
""" Perceptron: logical OR

"""
import numpy

# observations

inputs = numpy.array(
    [
        [0.0, 0.0],
        [0.0, 1.0],
        [1.0, 0.0],
        [1.0, 1.0]
        ]
    )

# classification of inputs
targets = numpy.array(
    [
        [1.0], [1.0], [1.0], [0.0]
        ]
    )

# number of neurons - for perceptron, 1
neurons = 1

# learning rate
eta = 0.25

# maximum number of steps
steps = 10

# irows is the number of observations
irows = numpy.shape(inputs)[0]

# construct the bias vector
bias = -1.0 * numpy.ones((irows, 1))

# construct inputs array with bias node appended
inputs = numpy.concatenate((bias, inputs), axis=1)
    
# define activation function
def g(x, w):
    a = numpy.dot(x, w)
    return numpy.where(a > 0.0, 1.0, 0.0)

# define SSE (2-norm)
def sse(x):
    x = numpy.dot(numpy.transpose(x), x)
    return numpy.sqrt(x).item()

# jcols is the number of inputs, inclusive of the bias
jcols = numpy.shape(inputs)[1]

# initialize weight array
weights = numpy.random.rand(jcols, neurons) * 0.1 - 0.05

cnt = 0

while (cnt < steps):
    outputs = g(inputs, weights)    
    weights += eta * numpy.dot(numpy.transpose(inputs), targets - outputs)
    print 'step: ', cnt
    print 'SSE: ', numpy.linalg.norm(weights)
    print 'SSE: ', sse(weights)
    print 'weights: ', numpy.transpose(weights)
    print 'outputs: ', numpy.transpose(outputs)
    
    cnt += 1
