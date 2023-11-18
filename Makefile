# COMMON CLI COMMANDS FOR DEVELOPMENT 

.PHONY: install_test
install_pure:
	@poetry install --with=development,testing

.PHONY: install_deps
install_test:
	@poetry install --with=development,linting,testing,docs

.PHONY: update_deps
update_deps:
	@poetry update --with=development,linting,testing,docs

.PHONY: test
test:
	@poetry run pytest tests

.PHONY: lint 
lint:
	@poetry run ruff check --fix quinn
