test: .FORCE
	python -m unittest discover -v -b

lint: .FORCE
	black forecast --preview
	black test --preview
	pylint forecast
	pylint test

.FORCE: