# Ayudas locales del repo docente (no es la solución de la práctica).
# Usad python3 -m pytest (el pytest del PATH a veces apunta a un intérprete roto).
.PHONY: help deps test-hito1 test-hito2 test-s19 test-s20 kit-hoy

PYTHON ?= python3
PYTEST  = $(PYTHON) -m pytest

help:
	@echo "Objetivos útiles:"
	@echo "  make deps                 # pip install requirements + requirements-dev"
	@echo "  make test-hito1 REPO=...  # pytest tests/hito1 --repo=\$$REPO"
	@echo "  make test-hito2 REPO=...  # pytest tests/hito2 --repo=\$$REPO"
	@echo "  make test-s19 REPO=...    # pytest tests/s19 --repo=\$$REPO"
	@echo "  make test-s20 REPO=...    # pytest tests/s20 --repo=\$$REPO (DECISION_S20.md)"
	@echo "  make kit-hoy              # zip con lo nuevo de la sesión 20"

deps:
	$(PYTHON) -m pip install -r requirements.txt -r requirements-dev.txt

test-hito1:
	@test -n "$(REPO)" || (echo "Usad: make test-hito1 REPO=/ruta/a/vuestro/clon" >&2; exit 1)
	$(PYTEST) -q tests/hito1 --repo="$(REPO)"

test-hito2:
	@test -n "$(REPO)" || (echo "Usad: make test-hito2 REPO=/ruta/a/vuestro/clon" >&2; exit 1)
	$(PYTEST) -q tests/hito2 --repo="$(REPO)"

test-s19:
	@test -n "$(REPO)" || (echo "Usad: make test-s19 REPO=/ruta/a/vuestro/clon" >&2; exit 1)
	$(PYTEST) -q tests/s19 --repo="$(REPO)"

test-s20:
	@test -n "$(REPO)" || (echo "Usad: make test-s20 REPO=/ruta/a/vuestro/clon" >&2; exit 1)
	$(PYTEST) -q tests/s20 --repo="$(REPO)"

# Empaqueta lo nuevo de hoy (sesión 20) + lo aún vigente de la 19.
kit-hoy:
	@rm -f kit_sesion_20.zip
	zip -r kit_sesion_20.zip \
		SESION_20.md ENCARGO_S20.md SESION_19.md ENCARGO_S19.md \
		README.md Makefile decision \
		streaming \
		requirements.txt requirements-dev.txt pytest.ini conftest.py \
		tests \
		.github/workflows/hito1-tests.yml \
		.github/workflows/hito2-tests.yml \
		-x '*.pyc' -x '*__pycache__*' -x '*.pytest_cache*'
	@echo "Generado: kit_sesion_20.zip"
