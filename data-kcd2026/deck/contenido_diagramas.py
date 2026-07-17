"""Los 8 diagramas, sobre el layout 4 (lienzo gris con timón y faro).

Formas nativas de PowerPoint: editables, nítidas a cualquier zoom y sin
dependencias de imagen. Título/kicker, tarjetas, flechas y zona del faro
vienen de kcd_estilo (sistema global).
"""
from pptx.dml.color import RGBColor
from pptx.enum.text import PP_ALIGN
from pptx.util import Pt

from kcd_estilo import *

CELESTE = RGBColor(0x7A, 0xB8, 0xF5)
AZUL_PALIDO = RGBColor(0xE3, 0xEE, 0xFB)
ROJO_PALIDO = RGBColor(0xF2, 0xD5, 0xD5)
ROJO_FONDO = RGBColor(0xF7, 0xE9, 0xE9)


def construir(slides):
    # ---------------------------------------- 06 pipeline con y sin gobierno
    s = slides[5]
    titulo(s, "El mismo pipeline, dos veces",
           "Arriba: el contrato es implícito. Abajo: el contrato manda.")

    for i, (etq, x) in enumerate([("Kafka", 0.7), ("Flink", 2.6), ("Sink", 4.5)]):
        rotular(bloque(s, x, 1.85, 1.5, 0.55, relleno=BLANCO, borde=GRIS_BORDE),
                etq, tamano=12)
        if i < 2:
            flecha(s, x + 1.5, 2.12, x + 1.9, 2.12, color=GRIS_FLECHA, grosor=1.5)
    caja(s, 6.3, 1.8, 3.2, 0.7,
         ["El contrato vive en el código.", "Nadie sabe por qué esa ventana."],
         tamano=11, color=TINTA_SUAVE)
    # La deriva ocurre EN el pipeline sin gobierno: conector rojo punteado
    # desde la base del Sink superior hasta el chip.
    flecha(s, 5.25, 2.4, 5.25, 2.55, color=ROJO, grosor=1.5, punteado=True)
    rotular(bloque(s, 4.3, 2.55, 1.9, 0.35, relleno=ROJO_PALIDO, borde=ROJO),
            "deriva silenciosa", tamano=9, color=ROJO)

    # Fila gobernada: arranca en x=1.5 (>= FARO_X) para liberar el haz del
    # faro, y su fila spec/gate baja a 3.20 para dejar >= 0.3" de aire con la
    # bandera roja de deriva (2.90) del pipeline de arriba.
    for i, (etq, x) in enumerate([("Kafka", 1.5), ("Flink", 3.4), ("Sink", 5.3)]):
        rotular(bloque(s, x, 3.9, 1.5, 0.55, relleno=BLANCO, borde=AZUL), etq, tamano=12)
        if i < 2:
            flecha(s, x + 1.5, 4.17, x + 1.9, 4.17)
    spec = bloque(s, 3.4, 3.2, 1.5, 0.5, relleno=AZUL)
    rotular(spec, "spec.md", tamano=11, color=BLANCO)
    flecha(s, 4.15, 3.7, 4.15, 3.9, color=AZUL)
    gate = bloque(s, 5.3, 3.2, 1.5, 0.5, relleno=VERDE)
    rotular(gate, "gate", tamano=11, color=BLANCO)
    flecha(s, 6.05, 3.7, 6.05, 3.9, color=VERDE)
    caja(s, 7.1, 3.7, 2.4, 0.85,
         ["El contrato es la fuente.", "El gate falla si el código deriva."],
         tamano=11, color=TINTA)
    notas(s, "Mismo stack, misma latencia, mismo costo. La unica diferencia es de "
             "donde sale el contrato y quien verifica que no derive. Todo lo que "
             "sigue es como construir la fila de abajo.")

    # ------------------------------------- 11 Foundation -> Core -> Consumer
    s = slides[10]
    titulo(s, "Foundation → Core → Consumer",
           "La jerarquía nunca se invierte. Una vista no ratifica lo que la origina.")

    capas = [
        ("Foundation", "Constitución · Ontología · KOM\nAutoridad. Ratificado en Git.",
         1.75, AZUL_OSCURO, BLANCO),
        ("Core", "Contratos · schemas · gates\nAplica el gobierno; no lo ratifica.",
         2.77, AZUL, BLANCO),
        # Tinta oscura sobre celeste: blanco ahi era el peor contraste del deck.
        ("Consumer", "CLI · demos · decks · pipelines\nConsume. Regenerable.",
         3.79, CELESTE, TINTA),
    ]
    # Columna en x=1.45 (>= FARO_X): la tarjeta Consumer baja de 4.2" y en
    # x menor pisaba el haz del faro del layout.
    for etq, desc, y, color, letra in capas:
        b = bloque(s, 1.45, y, 2.4, 0.72, relleno=color)
        rotular(b, etq, tamano=15, color=letra)
        caja(s, 4.1, y + 0.05, 3.3, 0.65, desc, tamano=11, color=TINTA)
    for y in (2.47, 3.49):
        flecha(s, 2.65, y, 2.65, y + 0.3, color=GRIS_FLECHA, grosor=1.5)

    prohibido = bloque(s, 7.7, 2.4, 1.8, 1.5, relleno=ROJO_FONDO, borde=ROJO)
    rotular(prohibido, ["✗", "Un consumer no", "puede gobernar", "a Foundation", "", "KOM-VR-08"],
            tamano=13, color=ROJO)
    notas(s, "Esta jerarquia decidio donde vive el generador de este deck. El deck es "
             "un consumer: consume la constitucion, el catalogo de ADRs y el "
             "claim-surface reales. Core no aprendio a hacer PowerPoint, y por eso "
             "sigue siendo stdlib puro y portable.")

    # ------------------------------- 13 la constitucion como proyeccion + canary
    s = slides[12]
    titulo(s, "La constitución no se escribe: se compila",
           "Y no cita a Foundation: falla si Foundation dejó de decir lo que ella afirma.")

    src = bloque(s, 0.7, 1.95, 2.2, 0.75, relleno=BLANCO, borde=AZUL)
    rotular(src, ["constitution", ".src.json"], tamano=11)
    comp = bloque(s, 3.5, 1.95, 2.2, 0.75, relleno=AZUL)
    # Nombre completo en una linea: partir en "compile_" dejaba un guion bajo
    # colgante que se leia como texto truncado.
    rotular(comp, ["compile_constitution.py"], tamano=10, color=BLANCO)
    out = bloque(s, 6.3, 1.95, 2.2, 0.75, relleno=BLANCO, borde=GRIS_BORDE)
    rotular(out, ["constitution.md", "derivado, no editar"], tamano=10)
    flecha(s, 2.9, 2.32, 3.5, 2.32)
    flecha(s, 5.7, 2.32, 6.3, 2.32)

    fnd = bloque(s, 0.7, 3.1, 2.2, 0.65, relleno=AZUL_PALIDO, borde=AZUL_OSCURO)
    rotular(fnd, ["Foundation", "ART-000, KOM…"], tamano=10)
    flecha(s, 1.8, 3.1, 1.8, 2.7, color=AZUL_OSCURO)
    caja(s, 3.5, 3.05, 5.0, 0.75,
         ["El compilador abre cada archivo de Foundation y verifica",
          "que el substring declarado en contains siga presente."],
         tamano=11)
    # Log a la derecha: libera la insignia KCD y respeta la zona del faro.
    log(s, 3.9, 3.9, 5.0,
        ['if article["contains"] not in fuente.casefold():',
         '    raise SystemExit(f"{id}: deriva en {source}")'],
        tamano=9, titulo="compile_constitution.py")
    caja(s, FARO_X, 4.15, 2.1, 0.6, ["Borrás una frase de", "Foundation → no compila."],
         tamano=11, negrita=True, color=ROJO)
    notas(s, "Este es el mecanismo que vamos a copiar para el pipeline. La "
             "constitucion declara, por articulo, un substring literal que debe "
             "existir en su fuente de Foundation. Si alguien edita ART-000 y borra "
             "esa frase, la constitucion no compila. Es un canary de deriva semantica.")

    # -------------------------------------- 16 EvidenceReceipt vs ApprovalReceipt
    s = slides[15]
    titulo(s, "Evidencia y aprobación son dos archivos",
           "Uno lo llena la máquina. El otro solo puede llenarlo una persona.")

    bloque(s, 0.7, 1.85, 3.7, 2.25, relleno=BLANCO, borde=AZUL)
    caja(s, 0.9, 1.98, 3.3, 0.3, "EvidenceReceipt v2", tamano=12, negrita=True)
    caja(s, 0.9, 2.35, 3.3, 1.25,
         ["EVR-<12 hex>",
          "actor.type: human | service | agent",
          "base_commit → head_commit",
          "evidence[]: path + sha256 + size",
          "verdict: passed | failed | blocked"],
         tamano=10, fuente=MONO, color=TINTA)
    caja(s, 0.9, 3.76, 3.3, 0.3, "La máquina puede emitirlo.", tamano=11)

    bloque(s, 5.6, 1.85, 3.7, 2.25, relleno=BLANCO, borde=VERDE)
    caja(s, 5.8, 1.98, 3.3, 0.3, "ApprovalReceipt v1", tamano=12, negrita=True)
    caja(s, 5.8, 2.35, 3.3, 1.25,
         ["APR-<12 hex>",
          'actor.type: {"const": "human"}',
          "authority_role: obligatorio",
          "evidence_receipt_digest: sha256",
          "verdict: accepted | rejected"],
         tamano=10, fuente=MONO, color=TINTA)
    caja(s, 5.8, 3.76, 3.3, 0.3, "Un agente NO valida contra este schema.", tamano=11)

    # La aprobacion apunta al recibo de evidencia (evidence_receipt_digest
    # vive en ApprovalReceipt): la flecha va de derecha a izquierda.
    flecha(s, 5.6, 3.05, 4.4, 3.05, color=VERDE, grosor=1.75)
    caja(s, 4.4, 3.2, 1.2, 0.3, "apunta por digest", tamano=9, color=TINTA_SUAVE,
         alineacion=PP_ALIGN.CENTER)
    caja(s, 1.5, 4.4, 7.0, 0.55,
         "ADR-0005 rechaza explícitamente “tratar CI o agentes como aprobadores: contradice autoridad humana”",
         tamano=11, negrita=True)
    notas(s, "Dos schemas, dos prefijos de id, dos actores. El ApprovalReceipt "
             "declara actor.type como const 'human': un agente no puede validar "
             "contra ese schema, literalmente. Y la aprobacion apunta por digest al "
             "recibo exacto que aprueba. No podes aprobar 'en general'.")

    # ------------------------------------------------------- 19 las 8 fases SDD
    s = slides[18]
    titulo(s, "El ciclo: 8 fases",
           "El checklist va cuarto, no al final. La spec es el punto de partida de todo.")

    fases = ["constitution", "specify", "clarify", "checklist", "plan", "tasks",
             "analyze", "implement"]
    x = 0.5
    for i, f in enumerate(fases):
        # Relleno azul solo para el nodo protagonista: la spec es el punto de
        # partida de todo; el resto en blanco con borde AZUL.
        protagonista = (i == 1)
        b = bloque(s, x, 1.9, 1.0, 0.75,
                   relleno=AZUL if protagonista else BLANCO,
                   borde=None if protagonista else AZUL)
        rotular(b, [str(i + 1), f], tamano=11, color=BLANCO if protagonista else TINTA)
        if i < 7:
            flecha(s, x + 1.0, 2.27, x + 1.13, 2.27, color=GRIS_FLECHA, grosor=1.5)
        x += 1.13

    caja(s, MARGEN, 3.05, 4.3, 1.1,
         [("specify — ", "“no inventar baseline, owner"),
          "ni verdad de dominio”",
          ("plan — ", "Constitution Check: 7 veredictos."),
          "VIOLA detiene el plan; el camino es el ADR.",
          ("tasks — ", "cobertura bidireccional: cada FR con"),
          "una tarea; ninguna tarea sin requisito."],
         tamano=10, tamano_prefijo=11)
    caja(s, COL_DER, 3.05, 4.3, 1.1,
         [("analyze — ", "“modo estrictamente solo lectura”."),
          "Bloquea con cualquier CRITICAL o HIGH.",
          "Propone correcciones sin aplicarlas.",
          ("implement — ", "“corregir siempre la fuente y"),
          "regenerar; no editar a mano vistas compiladas”."],
         tamano=10, tamano_prefijo=11)
    notas(s, "Ocho fases. Ojo con el orden: checklist va cuarto, antes de plan, no al "
             "final. El plan incluye un Constitution Check con veredicto por articulo, "
             "y un VIOLA detiene el plan: el camino no es una excepcion, es un ADR.")

    # -------------------------------------------------- 22 la cadena trazable
    s = slides[21]
    titulo(s, "La cadena trazable",
           "Cada eslabón resuelve al siguiente. Si uno falta, el gate no cierra.")

    eslabones = ["intención", "FR / SC", "plan", "tarea", "diff", "gate", "evidencia"]
    x = 0.55
    for i, e in enumerate(eslabones):
        # Relleno azul solo en el eslabón protagonista: la evidencia, que es
        # lo que cierra el gate; el resto blanco con borde AZUL.
        protagonista = (i == 6)
        b = bloque(s, x, 1.9, 1.15, 0.6,
                   relleno=AZUL if protagonista else BLANCO,
                   borde=None if protagonista else AZUL)
        rotular(b, e, tamano=11, color=BLANCO if protagonista else TINTA)
        if i < 6:
            flecha(s, x + 1.15, 2.2, x + 1.3, 2.2, color=GRIS_FLECHA, grosor=1.5)
        x += 1.3

    # Caption y matriz en x=FARO_X: el panel baja de 4.2" y en x=0.55 su
    # esquina inferior izquierda rozaba el haz del faro. Alinea ademas con la
    # cita de ADR-0002 de abajo, que ya vive en FARO_X.
    caja(s, FARO_X, 2.7, 8.0, 0.3, "Y se materializa en una matriz, no en una promesa:",
         tamano=12, negrita=True)
    log(s, FARO_X, 3.03, 8.0,
        ['| SC     | FR     | Tarea | Test/marker                          | Gate     | Evidencia          |',
         '|--------|--------|-------|--------------------------------------|----------|--------------------|',
         '| SC-002 | FR-002 | T007  | RevenueVentanaTest::sc002_sumaYConteo| test,e2e | evidence/sc-002.json|',
         '| SC-004 | FR-004 | T006  | RevenueVentanaTest::sc004_descarta   | test,e2e | evidence/sc-004.json|'],
        tamano=9, titulo="verification.md · matriz de trazabilidad")
    caja(s, FARO_X, 4.51, 8.05, 0.3,
         "“las rutas de evidencia se llenan al ejecutar la tarea y no constituyen un resultado anticipado” — ADR-0002",
         tamano=10, color=TINTA_SUAVE)
    notas(s, "La cadena es de ADR-0002. Lo importante es la ultima columna: la ruta de "
             "evidencia esta vacia hasta que la tarea corre. Una fila con evidencia "
             "vacia no es un resultado anticipado: es trabajo pendiente.")

    # --------------------------------------------- 26 arquitectura de la demo
    s = slides[25]
    titulo(s, "La demo: data-kcd2026",
           "Kafka → Flink → Kafka, con el contrato gobernado por la spec.")

    bloque(s, 0.55, 1.75, 3.0, 1.6, relleno=AZUL_PALIDO, borde=AZUL)
    caja(s, 0.7, 1.88, 2.7, 1.35,
         ["Gobierno (SDD)", "",
          "feature.spec.yaml ← fuente",
          "spec.md · FR-001..007",
          "plan.md · Constitution Check",
          "verification.md · matriz",
          "evidence/*.json"],
         tamano=10, negrita_lineas=[0])

    # El tercer nodo lleva el tópico protagonista completo: ancho propio.
    runtime = [("Kafka", "orders", 3.9, 1.5),
               ("Flink", "ventana 60s", 5.75, 1.5),
               ("Kafka", "orders_revenue_window", 7.55, 1.95)]
    for etq, sub, x, w in runtime:
        b = bloque(s, x, 2.05, w, 0.75, relleno=BLANCO, borde=AZUL)
        rotular(b, [etq, sub], tamano=11)
    flecha(s, 5.4, 2.42, 5.75, 2.42)
    flecha(s, 7.25, 2.42, 7.55, 2.42)

    gate = bloque(s, 0.55, 3.6, 3.0, 0.6, relleno=VERDE)
    rotular(gate, ["contract_check.py", "falla cerrado ante deriva"], tamano=10, color=BLANCO)
    flecha(s, 2.05, 3.35, 2.05, 3.6, color=AZUL)

    avsc = bloque(s, 3.9, 3.6, 5.5, 0.6, relleno=BLANCO, borde=GRIS_BORDE)
    rotular(avsc, ["orders_revenue_window.avsc — el contrato que ejecuta Flink",
                   "derivado de la spec, verificado por el gate"], tamano=10)
    flecha(s, 3.55, 3.9, 3.9, 3.9, color=VERDE)
    flecha(s, 6.5, 3.6, 6.5, 2.8, color=GRIS_FLECHA, grosor=1.5)

    caja(s, FARO_X, 4.45, 8.0, 0.35,
         "Compose real: broker · schema-registry · flink-jobmanager · flink-taskmanager",
         tamano=11, color=TINTA_SUAVE)
    notas(s, "El modulo se suma al repo data-evolutionary que ya llevamos a KCD 2025. "
             "La columna izquierda es lo nuevo: el carril de gobierno. La fila de la "
             "derecha es el pipeline de siempre. El gate es el puente.")
