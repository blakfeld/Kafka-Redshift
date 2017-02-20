.PHONY: venv
venv:
	@virtualenv venv
	@venv/bin/pip install -r requirements.txt

.PHONY: clean-libs
clean-libs:
	@rm -rf venv

.PHONY: rebulid-libs
rebuild-libs: clean-libs venv

.PHONY: clean-build
clean-build:
	@rm -rf build/
	@rm -rf dist/
	@rm -rf *.egg-info

.PHONY: clean
clean: clean-libs clean-build
	@find ./ -name "*.pyc" -exec rm -f {} \;
	@find ./ -name "*.pyo" -exec rm -f {} \;
	@find ./ -name "*~" -exec rm -f {} \;
	@find ./ -name "__pycache__" -exec rm -f {} \;
