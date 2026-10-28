import requests
from .config import load_config


def get_data(cnf: load_config) -> list:
    """ Extrae información realizando un HTTP hacia la API del banco internacional
        :param cnf: Instancia de la configuración cargada (load_config)
        :return: Lista de diccionarios {str, str} con el formato de todos los paises
    """
    source = next(s for s in cnf.sources if s.id == "countries_profile")
    response = requests.get(source.resolve_url(), timeout=30)
    response.raise_for_status()
    countries = response.json()
    mem = []
    for country in countries:
        mem_data = mem_data = {
            "name": country["name"]["official"],
            "iso2": country["cca2"],
            "iso3": country["cca3"],
        }
        mem.append(mem_data)
    print(f"Se han procesado {len(mem)}")
    return mem
        
if __name__ == "__main__":
    # Caso de uso
    cnf = load_config()
    print(get_data(cnf))