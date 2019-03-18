.PHONY: all assembly compile clean test

all: compile

assembly:
	sbt assembly

compile:
	sbt compile

clean:
	sbt clean

test:
	sbt run $(CL)
