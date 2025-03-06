build:
	python3 -m build

wheel:
	poetry build -f wheel

# deploy:
clean:
	find . -name Icon\* -exec rm {} \;
	-rm -rf dist
