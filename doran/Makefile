.DEFAULT_GOAL := run

help:
	@echo "⭐ Available commands..."
	@echo "  » clean (delete target folder)"
	@echo "  » build (build a über JAR)"
	@echo "  » run"
	@echo ""
	@echo "⏰ $(shell date)"

clean:
	rm -rf target/

build:
	# Usually the über JAR will be at...
	# doran/target/scala-2.12/doran-assembly-1.0.jar
	sbt assembly

run:
	sbt run

.PHONY: help clean build run
