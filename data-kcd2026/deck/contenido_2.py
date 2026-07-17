"""Slides 18-35: el ciclo SDD, la demo, los logs reales y el cierre.

Los bloques de log son transcripciones literales de esta sesión, no maquetas.
"""
from pptx.enum.text import PP_ALIGN
from pptx.util import Pt

from kcd_estilo import *


def construir(slides):
    # ------------------------------------------------------ 18 divisor: ciclo SDD
    s = slides[17]
    divisor(s, "02", ["El ciclo", "SDD"], "De la spec al gate, en ocho fases")
    notas(s, "Como se ve esto en la practica, fase por fase.")

    # ---------------------------------------------------------- 20 specify: FR/SC
    s = slides[19]
    reemplazar(buscar(s, "Title left aligned"), "specify: FR y SC", tamano=28)
    # El lead-in no es una lista: fuera el placeholder numerado (patrón s-29).
    quitar(s, "Bullet one")
    caja(s, MARGEN_AZUL, 1.7, 8.6, 0.55,
         ["Un FR declara. Un SC es falsable.", "Sin decisiones de implementación."],
         tamano=13, color=TINTA)
    log(s, MARGEN_AZUL, 2.4, 8.64,
        ['FR-004: una orden sin o_custkey o sin o_totalprice no debe abortar el job ni',
         '        contaminar una ventana; debe descartarse y contarse como descartada.',
         '',
         'SC-004: una orden con o_custkey nulo no aparece en la salida, el job sigue vivo',
         '        y el contador de descartes se incrementa en uno.'],
        tamano=9.5, titulo="spec.md · FR/SC")
    caja(s, MARGEN_AZUL, 4.1, 8.64, 0.8,
         ["El FR describe la intención. El SC dice cómo sabemos si mentimos.",
          "Si no podés escribir el SC, el FR todavía no está claro."],
         tamano=12, negrita=True, color=BLANCO)
    notas(s, "Este par sale de la spec real del pipeline. Fijense en la diferencia: "
             "el FR dice que debe pasar, el SC dice como se comprueba. El SC es lo "
             "que despues se convierte en un test con nombre.")

    # --------------------------------------------------- 21 plan: Constitution Check
    s = slides[20]
    reemplazar(buscar(s, "Title left aligned"), "plan: Constitution Check", tamano=26)
    quitar(s, "Bullet one")
    caja(s, MARGEN_AZUL, 1.7, 8.6, 0.55,
         ["Siete veredictos. Uno por artículo.", "VIOLA detiene el plan."],
         tamano=13, color=TINTA)
    log(s, MARGEN_AZUL, 2.35, 8.64,
        ['| # | Artículo                      | Veredicto | Evidencia                       |',
         '| I | El conocimiento manda         | PASS      | el contrato vive en la spec     |',
         '| II| Spec antes que artefacto      | PASS      | FR/SC antes del job             |',
         '|III| El canon crece por decisión   | N/A       | no hay frontera estructural     |',
         '| IV| Cero cifras sin fuente        | PASS      | sum declara fuente y límite     |',
         '| V | Una fuente, muchas vistas     | PASS      | 1 fuente, 3 representaciones    |',
         '| VI| La IA consume; el humano firma| PASS      | el agente generó; el owner firma|',
         '|VII| Privacidad por diseño         | PASS      | T0, TPC-H sintético, sin PII    |'],
        tamano=9.5, titulo="plan.md · Constitution Check")
    caja(s, MARGEN_AZUL, 4.55, 8.64, 0.3,
         "“Si el cambio es estructural y no existe ADR habilitante, detenerse y proponer el ADR”",
         tamano=11, color=BLANCO)
    caja(s, MARGEN_AZUL, 4.87, 8.64, 0.25, "— plan.md", tamano=10, color=BLANCO)
    notas(s, "Este es el Constitution Check real del plan del pipeline. No es un "
             "checkbox: cada veredicto exige evidencia. El III dice N/A porque el "
             "pipeline no toca una frontera estructural de Core. Cuando SI la tocamos "
             "—al decidir como generar este deck— tuvimos que escribir un ADR.")

    # -------------------------------------------------------- 23 los 15 gates
    s = slides[22]
    reemplazar(buscar(s, "Title left aligned"), "15 gates, fail-closed", tamano=27)
    # "15 gates" no puede numerarse 1-5: fuera el placeholder numerado.
    quitar(s, "Bullet one")
    caja(s, MARGEN_AZUL, 1.75, 4.3, 1.9,
         ["FND-PROJECTION · CATALOG-PROJECTION",
          "AGENT-PARITY · SDD-CONTRACT · KOM",
          "MONOREPO-STRUCTURE · TRACEABILITY",
          "BASELINE-SURFACE · CORE-CONFORMANCE",
          "CLAIM-SURFACE · CORE-DISTRIBUTION",
          "CORE-RELEASE-SEAL · CORE-BASE-DEMO",
          "TEST · VALIDATE"],
         tamano=11, color=BLANCO, interlineado=1.3)
    caja(s, COL_DER, 1.75, 4.35, 0.4, "El runner falla sobre sí mismo:", tamano=12,
         negrita=True, color=BLANCO)
    log(s, COL_DER, 2.2, 4.35,
        ['missing = REQUIRED_SCOPES - ids',
         'if missing:',
         '    raise ValueError(',
         '      f"gates obligatorios ausentes: {missing}")',
         '',
         '# y para cada gate requerido:',
         'if not {"pre-push","ci"}.issubset(scopes):',
         '    raise ValueError(f"{id}: scope obligatorio")'],
        tamano=9, titulo="run_gates.py")
    caja(s, COL_DER, 4.25, 4.35, 0.85,
         ["No podés desactivar un gate borrándolo del JSON:",
          "el runner exige que exista y que su scope incluya",
          "pre-push,ci. Debilitar el registro rompe el registro."],
         tamano=10, color=BLANCO)
    notas(s, "Quince gates, todos pre-push y ci menos VALIDATE. Lo elegante esta a la "
             "derecha: el runner valida el registro ANTES de ejecutar nada. Si borras "
             "un gate del json para que deje de molestar, el runner no arranca. "
             "Sin --scope y sin gates devuelve 1: nunca 'vacio = OK'.")

    # ------------------------------------------------------- 24 divisor: demo
    s = slides[23]
    divisor(s, "03", ["La demo"], "Kafka → Flink → Kafka, gobernado por la spec")
    notas(s, "Todo lo que sigue lo corrimos. Los logs son transcripciones, no maquetas.")

    # ---------------------------------------------------- 25 el caso data-kcd2026
    s = slides[24]
    reemplazar(buscar(s, "Title left aligned"), "El caso", tamano=30)
    b = buscar(s, "Bullet one")
    reemplazar(b, ["Revenue por cliente en ventana temporal",
                   "Kafka orders → Flink → orders_revenue_window",
                   "Sobre data-evolutionary, el repo de KCD 2025",
                   "Módulo nuevo: data-kcd2026",
                   "Flink 1.20.2 · Avro · Schema Registry"],
               tamano=13)
    b.top = pulg(1.75)
    b.width = pulg(4.3)
    caja(s, COL_DER, 1.75, 4.3, 0.35, "Tres decisiones que importan",
         tamano=12, negrita=True, color=BLANCO)
    caja(s, COL_DER, 2.2, 4.3, 2.2,
         ["GenericRecord, no clases generadas.",
          "Con clases, el contrato se congela en el",
          "bytecode y el gate se queda sin objeto.",
          "",
          "Lógica separada del wiring: SC-002..004",
          "se testean sin levantar Docker.",
          "",
          "double, no decimal → la spec NO reclama",
          "exactitud fiscal. Está en el .avsc."],
         tamano=11, color=BLANCO)
    notas(s, "Elegimos ventana temporal porque en 2025 ya mostramos CDC y join. Las "
             "tres decisiones de la derecha estan argumentadas en el plan.md, con sus "
             "alternativas rechazadas. La tercera es la mas honesta: degradamos decimal "
             "a double y lo declaramos, en vez de fingir exactitud fiscal.")

    # -------------------------------------------------- 27 la spec + Avro contrato
    s = slides[26]
    reemplazar(buscar(s, "Title left aligned"), "Una fuente, tres vistas", tamano=26)
    quitar(s, "Bullet one")
    caja(s, 0.65, 1.7, 8.6, 0.55,
         ["El .avsc no es la verdad.", "Es una representación."],
         tamano=13, color=TINTA)
    # Paneles hermanos: el derecho toma el alto del izquierdo (alto_min) para
    # que ambos bordes inferiores queden alineados, sin tocar el texto.
    lineas_spec = ['# feature.spec.yaml  ← la fuente',
                   'fields:',
                   '  - name: o_custkey',
                   '    type: long',
                   '  - name: sum_o_totalprice',
                   '    type: double']
    log(s, 0.65, 2.35, 4.17, lineas_spec, tamano=9, titulo="feature.spec.yaml")
    log(s, COL_DER, 2.35, 4.35,
        ['# orders_revenue_window.avsc',
         '{"name": "o_custkey",',
         ' "type": "long"},',
         '{"name": "sum_o_totalprice",',
         ' "type": "double"}'],
        tamano=9, titulo="orders_revenue_window.avsc",
        alto_min=log_alto(lineas_spec, tamano=9, titulo="feature.spec.yaml"))
    caja(s, 0.65, 4.2, 8.64, 0.75,
         ["Y una tercera: el texto de FR-002 en la spec en prosa.",
          "El gate compara las tres. Si agregás un campo al esquema y no lo explicás",
          "en el requisito, el requisito dejó de describir lo que el pipeline hace."],
         tamano=11, color=BLANCO)
    notas(s, "Tres representaciones del mismo contrato. El gate no compara dos: "
             "compara tres, incluida la prosa. Ese tercer chequeo es el que evita que "
             "la documentacion se vuelva decorativa.")

    # ------------------------------------------- 28 LOG: el canary en accion
    s = slides[27]
    titulo(s, "El canary, en 30 segundos",
           "Log real de esta sesión. Alguien “mejora” el esquema sin tocar la especificación.")
    log(s, 0.55, 1.56, 8.9,
        ['$ python3 data-kcd2026/tools/contract_check.py',
         '[contract] OK: 5 campos coinciden entre feature.spec.yaml, FR-002 y ...avsc',
         '[contract] contrato: o_custkey, window_start, window_end, sum_o_totalprice, order_count',
         '',
         "$ sed -i 's/\"sum_o_totalprice\"/\"total_revenue\"/' orders_revenue_window.avsc",
         '$ python3 data-kcd2026/tools/contract_check.py',
         '[contract] FAIL: el contrato derivo entre sus representaciones',
         "  - campo declarado en feature.spec.yaml y ausente del .avsc: 'sum_o_totalprice'",
         "  - campo presente en el .avsc y no declarado en feature.spec.yaml: 'total_revenue'",
         '',
         '  La especificacion manda. Corrige la fuente y regenera;',
         '  no ajustes el .avsc para que el gate calle.'],
        tamano=9.5, resaltar=["FAIL", "campo declarado", "campo presente"],
        titulo="contract_check.py")
    # Fila centrada en la banda clara entre el log (B=4.20) y la franja azul
    # inferior (top 4.98): antes quedaba pegada a la franja.
    caja(s, FARO_X, 4.48, 5.6, 0.35,
         "exit 1 — antes de compilar. Nombra el campo divergente.",
         tamano=14, negrita=True)
    chip = bloque(s, 7.35, 4.41, 1.35, 0.34, relleno=ROJO)
    rotular(chip, "FAIL → exit 1", tamano=10, color=BLANCO)
    notas(s, "DEMO EN VIVO. Correr el gate, romper el avsc con sed, correr de nuevo. "
             "Treinta segundos. El mensaje final es deliberado: 'no ajustes el .avsc "
             "para que el gate calle'. Es la tentacion natural y hay que nombrarla.")

    # ------------------------------------------------------------ 29 tres bugs
    s = slides[28]
    reemplazar(buscar(s, "Title left aligned"), "Los tests estaban verdes", tamano=26)
    # La lista numerada del layout no sirve aqui: el contenido son tres tarjetas,
    # no una enumeracion. Se elimina y se reconstruye la columna.
    quitar(s, "Bullet one")
    caja(s, 0.65, 1.85, 8.64, 0.3,
         "5/5 JUnit. Compilaba. El gate de contrato en verde. Y el pipeline estaba roto de tres maneras.",
         tamano=13, negrita=True)
    filas = [
        ("Thin jar de 14 KB", "sin shade: ClassNotFoundException al hacer submit", 2.4),
        ("orders.avsc ausente del classpath", "el job FALLÓ CERRADO y nombró el contrato faltante", 3.1),
        ("KryoException al primer dato", "RUNNING, log perfecto… y muerto al pasar el primer elemento", 3.8),
    ]
    for cabecera, desc, y in filas:
        bloque(s, 0.65, y, 0.1, 0.52, relleno=ROJO, radio=False)
        caja(s, 0.90, y + 0.02, 8.4, 0.5, [cabecera, desc],
             tamano=12, negrita_lineas=[0])
    caja(s, 0.65, 4.55, 8.64, 0.3,
         "Ningún test ni gate podía atraparlos. Un test verde prueba la lógica, no el despliegue.",
         tamano=12, negrita=True)
    notas(s, "Esto nos paso de verdad, hoy, construyendo esta demo. Los tres bugs "
             "sobrevivieron a 5 tests verdes y a un gate de contrato en verde. El "
             "tercero es el mas cruel: el job llegaba a RUNNING con el log correcto y "
             "moria al pasar el primer elemento fuera de la ventana, porque Flink no "
             "infiere el tipo de GenericRecord y cae a Kryo. Por eso T013 —correr "
             "contra el compose— existia como tarea separada, y por eso no declaramos "
             "'implemented' antes de cerrarla.")

    # ------------------------------------------------- 30 LOG: el pipeline e2e
    s = slides[29]
    titulo(s, "El pipeline, de verdad",
           "Cuatro fixtures a orders. Salida observada en orders_revenue_window.")
    log(s, 0.55, 1.6, 8.9,
        ['$ docker logs flink-jobmanager | grep CONTRATO -A6',
         '  origen  : topico=orders esquema=/model/orders.avsc',
         '  destino : topico=orders_revenue_window esquema=/model/orders_revenue_window.avsc',
         '  ventana : 20 segundos (processing time, no solapada)',
         '  gobierno: specs/001-orders-revenue-window/spec.md',
         '',
         '$ kafka-avro-console-consumer --topic orders_revenue_window --from-beginning',
         '{"o_custkey":7,"window_start":1784283820000,"window_end":1784283840000,',
         ' "sum_o_totalprice":150.0,"order_count":2}',
         '{"o_custkey":9,"window_start":1784283820000,"window_end":1784283840000,',
         ' "sum_o_totalprice":200.0,"order_count":1}'],
        tamano=9.5, titulo="sesión e2e · docker compose")
    # Bloque de conclusiones 0.16" mas arriba: la fila 2 invadia la franja
    # azul inferior del layout (top 4.98).
    caja(s, FARO_X, 4.2, 5.7, 0.3, "100.50 + 49.50 = 150.0 en 2 órdenes",
         tamano=12, negrita=True)
    chip = bloque(s, 7.35, 4.15, 1.35, 0.3, relleno=VERDE)
    rotular(chip, "SC-002 ✓", tamano=10, color=BLANCO)
    caja(s, FARO_X, 4.68, 5.75, 0.3,
         "La orden sin custkey (999.99) no aparece; si hubiera pasado: 1149.99",
         tamano=12, negrita=True)
    chip = bloque(s, 7.35, 4.63, 1.35, 0.3, relleno=VERDE)
    rotular(chip, "SC-004 ✓", tamano=10, color=BLANCO)
    notas(s, "El log de arranque declara el contrato: cualquiera que mire el log sabe "
             "que lee, que escribe y bajo que spec, sin abrir el codigo. Y la salida "
             "verifica dos criterios a ojo: la suma da 150 y la orden incompleta no "
             "esta. Si FR-004 hubiera fallado, veriamos 1149.99.")

    # ------------------------------------------ 31 divisor: lo que descubrimos
    s = slides[30]
    divisor(s, "04", ["Lo que", "descubrimos"], "Lo que el gobierno hizo solo")
    notas(s, "Y ahora lo que no planeamos: el gobierno se demostro solo.")

    # --------------------------------------------- 32 LOG: el gate rojo del ADR
    s = slides[31]
    titulo(s, "El gate se puso rojo solo",
           "Propusimos un ADR para decidir cómo generar este deck. Nadie programó lo que pasó después.")
    log(s, 0.55, 1.62, 8.9,
        ['$ python3 tools/validation/day_zero_demo_check.py .',
         "[FAIL] el release requiere ADR vigentes sin propuestas abiertas:",
         "       {'total': 15, 'accepted': 14, 'proposed': 1, 'deprecated': 0}",
         '',
         '$ # ADR-0015 aceptado por firma humana; catálogo recompilado',
         '$ python3 tools/validation/day_zero_demo_check.py .',
         '[core-demo] OK: Core 3.1.0 portable bajo ADR-0013; raíz única derivada'],
        tamano=9.5, resaltar=["FAIL", "proposed"], titulo="day_zero_demo_check.py")
    caja(s, 0.55, 3.65, 8.9, 0.55,
         ["El baseline se niega a regenerar su demo de “todo vigente”",
          "mientras haya una decisión estructural sin firmar."],
         tamano=15, negrita=True)
    # Caption 0.14" mas arriba: su borde inferior quedaba a 0.03" de la
    # franja azul inferior del layout.
    caja(s, FARO_X, 4.36, 8.05, 0.45,
         ["La IA redactó el ADR. El humano lo firmó. Solo entonces el gate cedió.",
          "generate_day_zero_demos.py:198 · commit 274812f"],
         tamano=11, color=TINTA_SUAVE)
    notas(s, "ESTE ES EL MOMENTO. No lo fabricamos: lo provocamos sin querer. Al "
             "escribir el ADR-0015, un check que nadie recordaba puso el gate en rojo. "
             "El sistema se niega a decir que esta completo cuando hay algo sin firmar "
             "sobre la mesa. Y volvio a verde por una firma humana, no por un parche. "
             "El ciclo completo quedo en el historial de git. Si hay tiempo: mostrarlo "
             "en vivo con git show 274812f.")

    # ------------------------------------------- 33 frontera de claims de la charla
    s = slides[32]
    reemplazar(buscar(s, "Title left aligned"), "Qué NO demostramos", tamano=27)
    b = buscar(s, "Bullet one")
    reemplazar(b, ["Exactitud fiscal: degradamos decimal a double",
                   "Datos tardíos o desordenados: usamos processing time",
                   "Tolerancia a fallos, exactly-once, rendimiento, escala",
                   "Operación en producción, adopción, outcome de negocio",
                   "EDAIOS gobernando pipelines de Flink en una organización real"],
               tamano=12)
    b.top = pulg(1.75)
    b.width = pulg(4.3)
    b.height = pulg(1.7)
    caja(s, COL_DER, 1.75, 4.3, 2.7,
         ["El importe agregado no es una cifra", "de negocio: es una suma sobre fixtures.", "",
          "Cuatro registros sintéticos, un broker,", "un taskmanager, en una laptop.", "",
          "Un pipeline verificado en un compose", "no es un pipeline en producción.", "",
          "Esta slide también es el patrón:", "declarar el límite es parte del método."],
         tamano=12, color=BLANCO)
    notas(s, "Practicamos lo que predicamos. Esta slide es un claim boundary, el mismo "
             "patron que EvidenceReceipt congela en su schema. Si nos fueramos sin "
             "esta slide, la demo verde se habria convertido en el claim, que es "
             "exactamente la enfermedad que vinimos a describir.")

    # ---------------------------- 34 statement: un gate verifica, un humano acepta
    s = slides[33]
    statement(s, ["“Un gate verifica;", "una persona", "autorizada acepta”"],
              "governance/README.md")
    # Aterrizaje del cierre en la mitad clara: el método en tres palabras y la
    # referencia que el deck ya cita en s-25. Sin URL: no está confirmada.
    caja(s, 5.5, 2.3, 3.9, 0.4, "spec → gate → evidencia",
         tamano=14, negrita=True, color=TINTA)
    caja(s, 5.5, 2.75, 3.9, 0.35, "data-evolutionary · módulo data-kcd2026",
         tamano=11, color=TINTA_SUAVE)
    notas(s, "Con esto cerramos. La IA puede descubrir, proponer, planificar, validar y "
             "producir borradores. La autoridad humana conserva priorizacion, "
             "aceptacion de riesgo, decisiones de arquitectura y veredicto de "
             "publicacion. El gate no acepta: verifica. Gracias.")
