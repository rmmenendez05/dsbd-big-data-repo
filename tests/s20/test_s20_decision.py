"""S20: comprueba DECISION_S20.md (campos y conclusiones de aula).

No arranca Spark ni Kafka. Lee el documento por casos: cada caso empieza
en un título de Markdown que nombra el informe (08:00) o la alerta (aforo).
Admite «campo: valor», «- **campo**: valor» y tildes en los nombres.
"""

from __future__ import annotations

import re
import unicodedata
from pathlib import Path

import pytest

pytestmark = pytest.mark.s20

NOMBRE = "DECISION_S20.md"
PLANTILLA = Path(__file__).resolve().parents[2] / "decision" / "plantilla_criterio.md"

FALTA = (
    "Falta DECISION_S20.md en la raíz de vuestro repo. Copiad la plantilla "
    "de cesar-andress/bigdata2026 (decision/plantilla_criterio.md), "
    "rellenadla y renombradla a DECISION_S20.md."
)

CAMPOS = (
    "latencia_util",
    "frescura_admitida",
    "decision",
    "consumidor_continuo",
    "conclusion_arquitectura",
    "por_que_no_la_otra",
)

HECHOS = ("08:00", "8:00", "02:00", "06:00", "2 min", "< 2", "<2", "180",
          "noche", "madrugada", "horas", "minutos", "segundos")


def _normal(texto: str) -> str:
    """Minúsculas, sin tildes y sin marcas de Markdown (* y `)."""
    sin_tildes = "".join(
        c for c in unicodedata.normalize("NFD", texto.lower())
        if unicodedata.category(c) != "Mn"
    )
    return sin_tildes.replace("*", "").replace("`", "")


def _sin_plantilla(texto: str) -> str:
    """Quita las líneas copiadas tal cual de la plantilla."""
    if not PLANTILLA.is_file():
        return texto
    copiadas = {l.strip() for l in PLANTILLA.read_text(encoding="utf-8").splitlines() if l.strip()}
    return "\n".join(l for l in texto.splitlines() if l.strip() not in copiadas)


def _casos(texto: str) -> dict[str, str]:
    """Trocea por títulos: 'a' = informe de las ocho, 'b' = alerta de aforo."""
    trozos: dict[str, str] = {}
    actual = None
    for linea in _normal(texto).splitlines():
        if linea.lstrip().startswith("#"):
            if re.search(r"informe|ocho|08:00", linea):
                actual = "a"
            elif re.search(r"alerta|aforo", linea):
                actual = "b"
            else:
                actual = None if "frase" in linea else actual
            continue
        if actual:
            trozos[actual] = trozos.get(actual, "") + linea + "\n"
    return trozos


def _valor(trozo: str, campo: str) -> str:
    m = re.search(rf"^[\s>\-]*{campo}\s*:[ \t]*(.*)$", trozo, flags=re.MULTILINE)
    return m.group(1).strip() if m else ""


def _buscar(repo_root: Path, git_root: Path) -> Path | None:
    for base in (repo_root, git_root):
        if (base / NOMBRE).is_file():
            return base / NOMBRE
    return None


def test_existe_decision_s20(repo_root: Path, git_root: Path):
    if _buscar(repo_root, git_root) is None:
        pytest.fail(FALTA)


@pytest.fixture(scope="module")
def texto(repo_root: Path, git_root: Path) -> str:
    path = _buscar(repo_root, git_root)
    if path is None:
        pytest.skip("sin DECISION_S20.md (ver test_existe_decision_s20)")
    return path.read_text(encoding="utf-8")


@pytest.fixture(scope="module")
def casos(texto: str) -> dict[str, str]:
    return _casos(texto)


def test_nombra_los_dos_casos(casos: dict[str, str]):
    assert "a" in casos, (
        "Falta el caso del informe: poned un título con «Informe» u «08:00» "
        "(como en la plantilla: «## Caso A · Informe de las ocho»)."
    )
    assert "b" in casos, (
        "Falta el caso de la alerta: poned un título con «Alerta» o «aforo» "
        "(como en la plantilla: «## Caso B · Alerta de aforo»)."
    )


@pytest.mark.parametrize("caso,nombre", [("a", "informe"), ("b", "alerta")])
def test_campos_rellenos(casos: dict[str, str], caso: str, nombre: str):
    if caso not in casos:
        pytest.skip("sin ese caso (ver test_nombra_los_dos_casos)")
    vacios = [c for c in CAMPOS if not _valor(casos[caso], c)]
    assert not vacios, (
        f"En el caso del {nombre} faltan o están vacíos: " + ", ".join(vacios) + "."
    )


def test_informe_concluye_batch(casos: dict[str, str]):
    if "a" not in casos:
        pytest.skip("sin caso del informe")
    valor = _valor(casos["a"], "conclusion_arquitectura")
    assert re.match(r"batch\b", valor), (
        "En el caso del informe, conclusion_arquitectura debe ser batch "
        f"(no streaming 24/7 obligatorio). Habéis puesto: «{valor or 'nada'}»."
    )


def test_alerta_concluye_streaming(casos: dict[str, str]):
    if "b" not in casos:
        pytest.skip("sin caso de la alerta")
    valor = _valor(casos["b"], "conclusion_arquitectura")
    assert re.match(r"streaming\b", valor), (
        "En el caso de la alerta de aforo, conclusion_arquitectura debe ser "
        f"streaming. Habéis puesto: «{valor or 'nada'}»."
    )


@pytest.mark.parametrize("caso,nombre", [("a", "informe"), ("b", "alerta")])
def test_no_solo_esloganes(casos: dict[str, str], caso: str, nombre: str):
    """Cada caso se apoya en algún dato: hora, frescura, minutos, volumen…"""
    if caso not in casos:
        pytest.skip("sin ese caso")
    assert any(h in casos[caso] for h in HECHOS), (
        f"El caso del {nombre} debe anclarse a hechos (hora de consulta, "
        "frescura, minutos, volumen…), no solo a adjetivos."
    )


def test_menciona_las_tres_frases_o_equivalente(texto: str):
    """Las frases del consumidor tienen que estar escritas, no solo copiadas."""
    propio = _normal(_sin_plantilla(texto))
    pistas = ("consumidor", "tarde", "antes", "quien", "dano", "accion", "ninguna")
    assert sum(1 for p in pistas if p in propio) >= 2, (
        "Incluid las tres frases del consumidor con vuestras palabras "
        "(quién/cuándo; qué pasa si llega tarde; qué haríais si llega antes). "
        "Copiar los títulos de la plantilla no cuenta."
    )
