#!/usr/bin/env python3
"""
generar_rips_sinteticos.py
==========================

Genera datos 100 % sintéticos con la estructura de los RIPS en JSON
(Resolución 2275 de 2023, Minsalud) más los insumos contractuales que
necesita una EPS para valorizar y auditar la facturación:

    data/raw/rips/AAAAMM/<NIT>_<FACTURA>.json   -> un RIPS por factura (capa Bronze)
    data/ref/prestadores.csv                     -> prestadores y modalidad de contrato
    data/ref/nota_tecnica.csv                    -> tarifas pactadas por prestador y tecnología
    data/ref/pgp_bandas.csv                      -> valor mensual y bandas de los contratos PGP
    data/ref/catalogo_cups.csv / catalogo_cie10.csv / catalogo_medicamentos.csv
    data/labels/anomalias_sembradas.csv          -> "verdad de terreno" por servicio (reglas de Silver)
    data/labels/anomalias_prestador_mes.csv      -> "verdad de terreno" por prestador-mes (análisis de Gold):
                                                    sobreejecución PGP y patrón de upcoding

NINGÚN dato viene de una base real: prestadores, NIT, documentos, fechas y
valores se inventan con una semilla fija, así que cualquiera puede reproducir
exactamente la misma base clonando el repositorio.

Los códigos CUPS, CIE-10 y DANE son catálogos públicos; aquí se usa una muestra
pequeña e ilustrativa. Los códigos de medicamento son sintéticos (no son CUM).
Verifica nombres de campos y valores permitidos contra el anexo técnico oficial
antes de presentarlo como "100 % conforme".

Uso:
    python generar_rips_sinteticos.py                      # 6 meses cerrados, escala 1
    python generar_rips_sinteticos.py --meses 12 --escala 20 --tasa-anomalia 0.04
"""

from __future__ import annotations

import argparse
import csv
import datetime as dt
import json
import random
from dataclasses import dataclass
from pathlib import Path

# ---------------------------------------------------------------------------
# Catálogos (muestras ilustrativas de catálogos públicos)
# ---------------------------------------------------------------------------

# código CUPS, descripción, tipo de servicio RIPS, valor base COP
CUPS = [
    ("890201", "Consulta de primera vez por medicina general", "consultas", 38000),
    ("890301", "Consulta de control por medicina general", "consultas", 32000),
    ("890202", "Consulta de primera vez por otras especialidades médicas", "consultas", 72000),
    ("890302", "Consulta de control por otras especialidades médicas", "consultas", 60000),
    ("902210", "Hemograma IV", "procedimientos", 21000),
    ("903841", "Glucosa en suero", "procedimientos", 7500),
    ("903895", "Creatinina en suero", "procedimientos", 8200),
    ("903818", "Colesterol total", "procedimientos", 8900),
    ("903868", "Triglicéridos", "procedimientos", 9800),
    ("907106", "Uroanálisis", "procedimientos", 9500),
    ("871121", "Radiografía de tórax", "procedimientos", 52000),
    ("881201", "Ecografía de abdomen total", "procedimientos", 118000),
]

# código CIE-10, descripción, sexo restringido (None = ambos)
CIE10 = [
    ("I10X", "Hipertensión esencial (primaria)", None),
    ("E119", "Diabetes mellitus tipo 2 sin complicaciones", None),
    ("E785", "Hiperlipidemia no especificada", None),
    ("J069", "Infección aguda de vías respiratorias superiores", None),
    ("K297", "Gastritis no especificada", None),
    ("M545", "Lumbago no especificado", None),
    ("N390", "Infección de vías urinarias, sitio no especificado", None),
    ("R51X", "Cefalea", None),
    ("J449", "Enfermedad pulmonar obstructiva crónica", None),
    ("Z000", "Examen médico general", None),
    ("O800", "Parto único espontáneo, presentación cefálica", "M"),
]

# código sintético, nombre, forma, concentración, valor unitario base COP
MEDICAMENTOS = [
    ("MED-SINT-0001", "Losartán", "Tableta", "50 mg", 180),
    ("MED-SINT-0002", "Metformina", "Tableta", "850 mg", 120),
    ("MED-SINT-0003", "Atorvastatina", "Tableta", "20 mg", 350),
    ("MED-SINT-0004", "Acetaminofén", "Tableta", "500 mg", 60),
    ("MED-SINT-0005", "Omeprazol", "Cápsula", "20 mg", 110),
    ("MED-SINT-0006", "Salbutamol", "Inhalador", "100 mcg/dosis", 9800),
    ("MED-SINT-0007", "Amoxicilina", "Cápsula", "500 mg", 210),
]

# código DANE municipio (público)
MUNICIPIOS = {
    "76001": "Cali", "76520": "Palmira", "76111": "Buga", "76834": "Tuluá",
    "76364": "Jamundí", "66001": "Pereira", "66170": "Dosquebradas",
}

# Prestadores ficticios: NIT inventado, nombre, municipio, modalidad
PRESTADORES = [
    ("900100001", "IPS Ficticia Valle Salud", "76001", "EVENTO"),
    ("900100002", "Clínica Sintética del Pacífico", "76001", "EVENTO"),
    ("900100003", "Laboratorio Demo Andino", "76520", "EVENTO"),
    ("900100004", "ESE Hospital Ejemplo Norte", "76834", "PGP"),
    ("900100005", "Centro Médico Prueba Buga", "76111", "PGP"),
    ("900100006", "IPS Modelo Risaralda", "66001", "PGP"),
    ("900100007", "Unidad Básica Simulada", "66170", "CAPITA"),
    ("900100008", "Clínica Imaginaria del Sur", "76364", "EVENTO"),
]

TIPOS_ANOMALIA = [
    "SOBRECOSTO",            # cobra por encima de la tarifa pactada en la NT
    "NO_CONTRATADO",         # tecnología fuera de la NT del prestador
    "DUPLICADO",             # mismo usuario, código y fecha facturado dos veces
    "FECHA_INVALIDA",        # atención antes del nacimiento o en el futuro
    "DX_SEXO_INCOMPATIBLE",  # diagnóstico obstétrico en un hombre
]


@dataclass
class Usuario:
    tipo_doc: str
    num_doc: str
    tipo_usuario: str
    fecha_nac: dt.date
    sexo: str
    municipio: str
    zona: str


# ---------------------------------------------------------------------------
# Utilidades
# ---------------------------------------------------------------------------

def tipo_documento(edad: int) -> str:
    if edad < 7:
        return "RC"
    if edad < 18:
        return "TI"
    return "CC"


def nuevo_usuario(rng: random.Random, municipio: str, hoy: dt.date) -> Usuario:
    edad = min(int(rng.expovariate(1 / 38)), 95)
    fecha_nac = hoy - dt.timedelta(days=edad * 365 + rng.randint(0, 364))
    return Usuario(
        tipo_doc=tipo_documento(edad),
        num_doc=str(rng.randint(1_000_000_000, 1_999_999_999)),
        tipo_usuario=rng.choices(["01", "02", "04"], weights=[45, 20, 35])[0],
        fecha_nac=fecha_nac,
        sexo=rng.choice(["H", "M"]),
        municipio=municipio,
        zona=rng.choices(["01", "02"], weights=[85, 15])[0],
    )


def fecha_en_mes(rng: random.Random, anio: int, mes: int) -> dt.date:
    siguiente = dt.date(anio + (mes == 12), mes % 12 + 1, 1)
    dias = (siguiente - dt.date(anio, mes, 1)).days
    return dt.date(anio, mes, rng.randint(1, dias))


def dx_para(rng: random.Random, sexo: str) -> str:
    validos = [c for c, _, s in CIE10 if s is None or s == sexo]
    return rng.choice(validos)


# ---------------------------------------------------------------------------
# Insumos contractuales
# ---------------------------------------------------------------------------

def construir_nota_tecnica(rng: random.Random) -> list[dict]:
    filas = []
    for nit, _, _, modalidad in PRESTADORES:
        # cada prestador contrata un subconjunto de tecnologías
        cups = rng.sample(CUPS, k=rng.randint(7, len(CUPS) - 1))
        meds = rng.sample(MEDICAMENTOS, k=rng.randint(4, len(MEDICAMENTOS)))
        for codigo, desc, tipo, base in cups:
            filas.append({
                "nit_prestador": nit, "tipo_tecnologia": tipo, "codigo": codigo,
                "descripcion": desc, "modalidad": modalidad,
                "valor_unitario_nt": round(base * rng.uniform(0.9, 1.2), -2),
                "frecuencia_mensual_esperada": rng.randint(20, 400),
            })
        for codigo, nombre, forma, conc, base in meds:
            filas.append({
                "nit_prestador": nit, "tipo_tecnologia": "medicamentos", "codigo": codigo,
                "descripcion": f"{nombre} {conc} {forma}", "modalidad": modalidad,
                "valor_unitario_nt": round(base * rng.uniform(0.9, 1.15), 0),
                "frecuencia_mensual_esperada": rng.randint(100, 3000),
            })
    return filas


def construir_bandas_pgp(rng: random.Random, valor_mes: dict, sobreejecucion: set) -> list[dict]:
    """El valor PGP pactado se calibra con el comportamiento normal del prestador
    (mediana de sus meses sin sobreejecución), como haría una EPS al negociar con historia."""
    bandas = []
    for nit, _, _, modalidad in PRESTADORES:
        if modalidad != "PGP":
            continue
        normales = sorted(v for (n, p), v in valor_mes.items() if n == nit and (n, p) not in sobreejecucion)
        mediana = normales[len(normales) // 2]
        valor = mediana * rng.uniform(0.99, 1.01)
        bandas.append({
            "nit_prestador": nit,
            "valor_mensual_pgp": round(valor, -3),
            "limite_inferior": round(valor * 0.9, -3),
            "limite_superior": round(valor * 1.1, -3),
        })
    return bandas


def elegir_prestador_upcoding(nota: list[dict]) -> str:
    """Prestador por evento con mayor brecha entre su consulta más cara y la más barata."""
    brechas = {}
    for nit, _, _, modalidad in PRESTADORES:
        if modalidad != "EVENTO":
            continue
        valores = [f["valor_unitario_nt"] for f in nota
                   if f["nit_prestador"] == nit and f["tipo_tecnologia"] == "consultas"]
        if len(valores) >= 2:
            brechas[nit] = max(valores) / min(valores)
    return max(brechas, key=brechas.get)


# ---------------------------------------------------------------------------
# Generación de RIPS
# ---------------------------------------------------------------------------

class Generador:
    def __init__(self, rng: random.Random, nota: list[dict], tasa_anomalia: float):
        self.rng = rng
        self.tasa = tasa_anomalia
        self.nt = {}
        for f in nota:
            self.nt.setdefault(f["nit_prestador"], {}).setdefault(f["tipo_tecnologia"], []).append(f)
        self.etiquetas: list[dict] = []
        self.ultimo_servicio: dict | None = None  # para sembrar duplicados
        self.upcoding = False  # prestador-mes con patrón de consultas infladas

    # -- servicios -----------------------------------------------------------
    def _elegir(self, nit: str, tipo: str) -> tuple[dict, str | None]:
        """Devuelve (fila de NT o tecnología fuera de NT, anomalía sembrada)."""
        contratadas = self.nt[nit].get(tipo, [])
        if tipo == "consultas" and self.upcoding and self.rng.random() < 0.6:
            # cada consulta cobra la tarifa correcta: solo el patrón agregado es anómalo
            return max(contratadas, key=lambda f: f["valor_unitario_nt"]), None
        if self.rng.random() < self.tasa / len(TIPOS_ANOMALIA):
            codigos = {f["codigo"] for f in contratadas}
            if tipo == "medicamentos":
                fuera = [m for m in MEDICAMENTOS if m[0] not in codigos]
                if fuera:
                    c = self.rng.choice(fuera)
                    return {"codigo": c[0], "descripcion": c[1], "valor_unitario_nt": c[4] * 1.1}, "NO_CONTRATADO"
            else:
                fuera = [c for c in CUPS if c[2] == tipo and c[0] not in codigos]
                if fuera:
                    c = self.rng.choice(fuera)
                    return {"codigo": c[0], "descripcion": c[1], "valor_unitario_nt": c[3] * 1.1}, "NO_CONTRATADO"
        return self.rng.choice(contratadas), None

    def _valor(self, base: float, anomalia: str | None) -> tuple[int, str | None]:
        if anomalia is None and self.rng.random() < self.tasa / len(TIPOS_ANOMALIA):
            return int(round(base * self.rng.uniform(1.3, 2.5), -2)), "SOBRECOSTO"
        return int(round(base * self.rng.uniform(0.98, 1.0))), anomalia

    def _fecha(self, u: Usuario, fecha: dt.date, anomalia: str | None) -> tuple[dt.date, str | None]:
        if anomalia is None and self.rng.random() < self.tasa / len(TIPOS_ANOMALIA):
            mala = u.fecha_nac - dt.timedelta(days=self.rng.randint(10, 400))
            return mala, "FECHA_INVALIDA"
        return fecha, anomalia

    def _dx(self, u: Usuario, anomalia: str | None) -> tuple[str, str | None]:
        if anomalia is None and u.sexo == "H" and self.rng.random() < self.tasa / len(TIPOS_ANOMALIA):
            return "O800", "DX_SEXO_INCOMPATIBLE"
        return dx_para(self.rng, u.sexo), anomalia

    def servicio(self, nit: str, tipo: str, u: Usuario, fecha: dt.date,
                 consecutivo: int, factura: str, usuario_consec: int) -> dict:
        fila, anomalia = self._elegir(nit, tipo)
        fecha, anomalia = self._fecha(u, fecha, anomalia)
        dx, anomalia = self._dx(u, anomalia)
        ts = dt.datetime.combine(fecha, dt.time(self.rng.randint(6, 19), self.rng.choice([0, 15, 30, 45])))
        comun = {
            "codPrestador": nit + "01",
            "numAutorizacion": None if self.rng.random() < 0.6 else str(self.rng.randint(10**8, 10**9)),
            "tipoDocumentoIdentificacion": u.tipo_doc,
            "numDocumentoIdentificacion": u.num_doc,
            "codDiagnosticoPrincipal": dx,
            "conceptoRecaudo": "05",
            "valorPagoModerador": 0,
            "numFEVPagoModerador": None,
            "consecutivo": consecutivo,
        }
        if tipo == "medicamentos":
            cantidad = self.rng.choice([10, 14, 28, 30, 60, 90]) if "Inhalador" not in fila["descripcion"] else 1
            vr_unit, anomalia = self._valor(fila["valor_unitario_nt"], anomalia)
            reg = {**comun,
                   "idMIPRES": None,
                   "fechaDispensAdmon": ts.strftime("%Y-%m-%d %H:%M"),
                   "codDiagnosticoRelacionado": None,
                   "tipoMedicamento": "01",
                   "codTecnologiaSalud": fila["codigo"],
                   "nomTecnologiaSalud": fila["descripcion"],
                   "cantidadMedicamento": cantidad,
                   "diasTratamiento": min(cantidad, 30),
                   "vrUnitMedicamento": vr_unit,
                   "vrServicio": vr_unit * cantidad}
        else:
            vr, anomalia = self._valor(fila["valor_unitario_nt"], anomalia)
            reg = {**comun,
                   "fechaInicioAtencion": ts.strftime("%Y-%m-%d %H:%M"),
                   "modalidadGrupoServicioTecSal": "01",
                   "grupoServicios": "01" if tipo == "consultas" else "02",
                   "finalidadTecnologiaSalud": self.rng.choice(["11", "15", "16"]),
                   "vrServicio": vr}
            if tipo == "consultas":
                reg.update({"codConsulta": fila["codigo"], "causaMotivoAtencion": "38",
                            "tipoDiagnosticoPrincipal": self.rng.choice(["01", "02", "03"])})
            else:
                reg.update({"idMIPRES": None, "codProcedimiento": fila["codigo"],
                            "viaIngresoServicioSalud": "01", "codComplicacion": None})

        clave = {"numFactura": factura, "usuario_consecutivo": usuario_consec,
                 "tipo_servicio": tipo, "consecutivo": consecutivo}
        if anomalia:
            self.etiquetas.append({**clave, "nit_prestador": nit, "num_documento": u.num_doc,
                                   "codigo": fila["codigo"], "tipo_anomalia": anomalia})
        self.ultimo_servicio = {"tipo": tipo, "reg": reg, "usuario": u}
        return reg

    # -- facturas ------------------------------------------------------------
    def factura(self, nit: str, num_factura: str, usuarios: list[Usuario],
                anio: int, mes: int, servicios_por_usuario: tuple[int, int]) -> dict:
        bloque_usuarios = []
        for i, u in enumerate(usuarios, start=1):
            servicios = {"consultas": [], "procedimientos": [], "medicamentos": []}
            n = self.rng.randint(*servicios_por_usuario)
            fecha = fecha_en_mes(self.rng, anio, mes)
            for _ in range(n):
                tipo = self.rng.choices(["consultas", "procedimientos", "medicamentos"],
                                        weights=[40, 35, 25])[0]
                consec = len(servicios[tipo]) + 1
                servicios[tipo].append(self.servicio(nit, tipo, u, fecha, consec, num_factura, i))
                # duplicado: repetir el mismo servicio (mismo usuario, código y fecha)
                if self.rng.random() < self.tasa / len(TIPOS_ANOMALIA):
                    dup = dict(servicios[tipo][-1])
                    dup["consecutivo"] = len(servicios[tipo]) + 1
                    servicios[tipo].append(dup)
                    codigo = dup.get("codConsulta") or dup.get("codProcedimiento") or dup.get("codTecnologiaSalud")
                    self.etiquetas.append({"numFactura": num_factura, "usuario_consecutivo": i,
                                           "tipo_servicio": tipo, "consecutivo": dup["consecutivo"],
                                           "nit_prestador": nit, "num_documento": u.num_doc,
                                           "codigo": codigo, "tipo_anomalia": "DUPLICADO"})
            bloque_usuarios.append({
                "tipoDocumentoIdentificacion": u.tipo_doc,
                "numDocumentoIdentificacion": u.num_doc,
                "tipoUsuario": u.tipo_usuario,
                "fechaNacimiento": u.fecha_nac.isoformat(),
                "codSexo": u.sexo,
                "codPaisResidencia": "170",
                "codMunicipioResidencia": u.municipio,
                "codZonaTerritorialResidencia": u.zona,
                "incapacidad": "NO",
                "consecutivo": i,
                "codPaisOrigen": "170",
                "servicios": {k: v for k, v in servicios.items() if v},
            })
        return {
            "numDocumentoIdObligado": nit,
            "numFactura": num_factura,
            "tipoNota": None,
            "numNota": None,
            "usuarios": bloque_usuarios,
        }


# ---------------------------------------------------------------------------
# Escritura
# ---------------------------------------------------------------------------

def escribir_csv(ruta: Path, filas: list[dict]) -> None:
    ruta.parent.mkdir(parents=True, exist_ok=True)
    if not filas:
        return
    with ruta.open("w", newline="", encoding="utf-8") as fh:
        w = csv.DictWriter(fh, fieldnames=list(filas[0].keys()))
        w.writeheader()
        w.writerows(filas)


def main() -> None:
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--salida", default="data", help="carpeta de salida (por defecto ./data)")
    ap.add_argument("--meses", type=int, default=6, help="meses a generar desde --inicio")
    ap.add_argument("--inicio", default=None,
                    help="primer mes AAAA-MM (por defecto: el necesario para terminar en el último mes cerrado)")
    ap.add_argument("--escala", type=float, default=1.0, help="multiplicador de volumen")
    ap.add_argument("--tasa-anomalia", type=float, default=0.03, help="proporción aprox. de servicios anómalos")
    ap.add_argument("--semilla", type=int, default=2275)
    args = ap.parse_args()

    rng = random.Random(args.semilla)
    salida = Path(args.salida)
    if args.inicio:
        anio0, mes0 = map(int, args.inicio.split("-"))
    else:
        # datos históricos: el último mes generado es el mes anterior al actual
        actual = dt.date.today()
        indice = actual.year * 12 + (actual.month - 1) - args.meses
        anio0, mes0 = indice // 12, indice % 12 + 1
    hoy = dt.date(anio0, mes0, 1)

    nota = construir_nota_tecnica(rng)
    gen = Generador(rng, nota, args.tasa_anomalia)

    # población afiliada por municipio (se reutiliza mes a mes, como en la vida real)
    poblacion = {m: [nuevo_usuario(rng, m, hoy) for _ in range(int(800 * args.escala))] for m in MUNICIPIOS}

    meses = [(anio0 + (mes0 - 1 + k) // 12, (mes0 - 1 + k) % 12 + 1) for k in range(args.meses)]
    periodos = [f"{a}{m:02d}" for a, m in meses]

    # anomalías a nivel prestador-mes (invisibles fila por fila)
    nits_pgp = [n for n, _, _, mod in PRESTADORES if mod == "PGP"]
    candidatos = [(n, p) for n in nits_pgp for p in periodos[3:]]  # deja historia antes de sembrar
    sobreejecucion = set(rng.sample(candidatos, k=min(3, len(candidatos))))
    nit_upcoding = elegir_prestador_upcoding(nota)
    upcoding = {(nit_upcoding, p) for p in periodos[-3:]} if len(periodos) >= 6 else set()
    etiquetas_pm = (
        [{"nit_prestador": n, "periodo": p, "tipo_anomalia": "SOBREEJECUCION_PGP"} for n, p in sorted(sobreejecucion)]
        + [{"nit_prestador": n, "periodo": p, "tipo_anomalia": "UPCODING_PATRON"} for n, p in sorted(upcoding)]
    )

    valor_mes: dict[tuple[str, str], float] = {}
    total_facturas = total_servicios = 0
    contador = 0
    for (anio, mes), periodo in zip(meses, periodos):
        carpeta = salida / "raw" / "rips" / periodo
        carpeta.mkdir(parents=True, exist_ok=True)
        for nit, _, municipio, modalidad in PRESTADORES:
            pob = poblacion[municipio]
            gen.upcoding = (nit, periodo) in upcoding
            if modalidad == "EVENTO":
                # una factura por atención
                n_fact = int(rng.randint(40, 90) * args.escala)
                lotes = [[rng.choice(pob)] for _ in range(n_fact)]
                rango = (1, 4)
            else:
                # PGP / cápita: facturas mensuales por lotes de usuarios, volumen estable
                base = rng.gauss(225, 4) * (1.4 if (nit, periodo) in sobreejecucion else 1.0)
                n_usuarios = int(base * args.escala)
                elegidos = rng.sample(pob, k=min(n_usuarios, len(pob)))
                lotes = [elegidos[i:i + 50] for i in range(0, len(elegidos), 50)]
                rango = (1, 3)
            for lote in lotes:
                contador += 1
                num = f"FE{contador:07d}"
                rips = gen.factura(nit, num, lote, anio, mes, rango)
                regs = [s for u in rips["usuarios"] for v in u["servicios"].values() for s in v]
                total_servicios += len(regs)
                valor_mes[(nit, periodo)] = valor_mes.get((nit, periodo), 0) + sum(s["vrServicio"] for s in regs)
                (carpeta / f"{nit}_{num}.json").write_text(
                    json.dumps(rips, ensure_ascii=False, indent=2), encoding="utf-8")
                total_facturas += 1
    gen.upcoding = False

    escribir_csv(salida / "ref" / "prestadores.csv",
                 [{"nit_prestador": n, "nombre": nom, "cod_municipio": m,
                   "municipio": MUNICIPIOS[m], "modalidad": mod} for n, nom, m, mod in PRESTADORES])
    escribir_csv(salida / "ref" / "nota_tecnica.csv", nota)
    escribir_csv(salida / "ref" / "pgp_bandas.csv", construir_bandas_pgp(rng, valor_mes, sobreejecucion))
    escribir_csv(salida / "ref" / "catalogo_cups.csv",
                 [{"codigo": c, "descripcion": d, "tipo_servicio": t} for c, d, t, _ in CUPS])
    escribir_csv(salida / "ref" / "catalogo_cie10.csv",
                 [{"codigo": c, "descripcion": d, "sexo_restringido": s or ""} for c, d, s in CIE10])
    escribir_csv(salida / "ref" / "catalogo_medicamentos.csv",
                 [{"codigo": c, "nombre": n, "forma": f, "concentracion": k} for c, n, f, k, _ in MEDICAMENTOS])
    escribir_csv(salida / "labels" / "anomalias_sembradas.csv", gen.etiquetas)
    escribir_csv(salida / "labels" / "anomalias_prestador_mes.csv", etiquetas_pm)

    print(f"Facturas: {total_facturas:,} | servicios: {total_servicios:,} | "
          f"anomalías sembradas: {len(gen.etiquetas):,} "
          f"({len(gen.etiquetas) / max(total_servicios, 1):.1%}) | "
          f"anomalías prestador-mes: {len(etiquetas_pm)}")
    print(f"Salida en: {salida.resolve()}")


if __name__ == "__main__":
    main()
