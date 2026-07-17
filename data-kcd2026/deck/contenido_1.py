"""Slides 1-17: apertura, el problema, que es EDAIOS, los invariantes."""
from pptx.enum.text import PP_ALIGN
from pptx.util import Pt

from kcd_estilo import *


def construir(slides):
    # ---------------------------------------------------------------- 02 titulo
    s = slides[1]
    # Cortes explicitos en 4 lineas: el placeholder mide ~4.6" y envuelve mal
    # cualquier corte automatico ("Construyendo Data / Pipelines" con huerfana).
    reemplazar(buscar(s, "Kubernetes a Escala"),
               ["Construyendo", "Data Pipelines", "con Apache Flink y",
                "Spec Driven Development"],
               tamano=26)
    # Los rotulos de speaker del template son de una linea; con dos nombres hay
    # que ensancharlos y separarlos o se solapan.
    nombre = buscar(s, "César Lorca Bacian")
    nombre.width = pulg(6.0)
    nombre.top = pulg(3.55)
    reemplazar(nombre, "David Susanibar   ·   Jorge Alor", tamano=15)
    rol = buscar(s, "SUSE Presales")
    rol.width = pulg(6.0)
    rol.top = pulg(3.95)
    reemplazar(rol, "Head Data & AI @ UTP   ·   Software Architect @ BCP", tamano=11)
    notas(s, "La IA generativa nos dejo escribir pipelines mas rapido que nunca. "
             "La pregunta de hoy no es como generar mas codigo: es como no perder "
             "calidad, trazabilidad y mantenibilidad mientras lo hacemos.")

    # ------------------------------------------------------------ 03 quienes somos
    s = slides[2]
    reemplazar(buscar(s, "Title left aligned"), "Quiénes somos", tamano=30)
    reemplazar(buscar(s, "Bullet one"),
               ["David Susanibar — Head of Data & AI en UTP",
                "Jorge Alor — Software Architect en BCP",
                "Committer y contribuidor en proyectos de datos OSS",
                "Esta charla nace de un repo real, no de un slide"],
               tamano=15)
    notas(s, "Presentacion breve. El punto: lo que van a ver salio de codigo que "
             "corrimos, no de un ejemplo de documentacion.")

    # ------------------------------------- 04 statement: velocidad sin contrato
    s = slides[3]
    statement(s,
              ["“La velocidad sin", "contrato desplaza", "decisiones a código", "y conversación”"],
              "ADR-0002 · EDAIOS")
    notas(s, "Esta frase es de un ADR real del repo que vamos a mostrar. "
             "Describe la enfermedad: cuando no hay contrato, la decision no "
             "desaparece; se muda al codigo y al chat, donde nadie la revisa.")

    # ------------------------------------------------- 05 patologia del pipeline
    s = slides[4]
    reemplazar(buscar(s, "Title left aligned"), "La patología del pipeline", tamano=28)
    b = buscar(s, "Bullet one")
    reemplazar(b, ["El contrato del stream vive en el código",
                   "El porqué de esa ventana vive en un Slack borrado",
                   "El .avsc y el doc dicen cosas distintas",
                   "La demo verde se convierte en el claim de producción"],
               tamano=14)
    b.width = pulg(4.3)
    notas(s, "Cuatro sintomas que todos reconocemos. El cuarto es el mas caro: "
             "'funciono en la demo' termina siendo la evidencia de que esta listo "
             "para produccion. Volvemos a este punto al final, con datos.")

    # ----------------------------------------------- 07 IA = deuda a escala
    s = slides[6]
    reemplazar(buscar(s, "Title left aligned"), "La IA no crea el problema", tamano=28)
    b = buscar(s, "Bullet one")
    reemplazar(b, ["Lo acelera. Genera pipelines más rápido de lo que podés revisarlos",
                   "Un agente no sabe qué decidió tu equipo hace seis meses",
                   "Sin contrato explícito, la IA inventa uno plausible",
                   "Velocidad sin gobierno = deuda técnica a escala industrial"],
               tamano=14)
    b.width = pulg(4.3)
    notas(s, "No es una charla anti-IA: usamos un agente para generar el job de Flink "
             "que van a ver. El punto es que la IA amplifica lo que ya tenias. Si tu "
             "contrato es implicito, ahora tenes deuda implicita a mayor velocidad.")

    # ---------------------------------------------------- 08 divisor: que es EDAIOS
    s = slides[7]
    divisor(s, "01", ["¿Qué es", "EDAIOS?"], "El sistema de gobierno")
    notas(s, "Primera parte: el sistema de gobierno. Segunda parte: como lo aplicamos "
             "a un pipeline de Flink.")

    # ---------------------------------------------------------- 09 knowledge first
    s = slides[8]
    reemplazar(buscar(s, "Title left aligned"), "Knowledge First", tamano=30)
    # La cita no es una lista: sale del placeholder numerado y va en su caja.
    caja(s, MARGEN_AZUL, 1.9, 4.4, 0.9,
         ["“El conocimiento es el producto principal",
          "y el software su consecuencia”"],
         tamano=15, negrita=True, color=BLANCO)
    b = buscar(s, "Bullet one")
    reemplazar(b, ["El código puede cambiar. La arquitectura puede evolucionar.",
                   "Las herramientas pueden reemplazarse.",
                   "El conocimiento debe preservarse."],
               tamano=13)
    b.top = pulg(2.9)
    b.width = pulg(4.4)
    caja(s, MARGEN_AZUL, 4.6, 4.4, 0.35, "ART-008 · ART-000 · Foundation",
         tamano=10, color=BLANCO)
    notas(s, "Definicion canonica del glosario de Foundation. No es una metafora: "
             "el repo trata al conocimiento como el artefacto versionado y al codigo "
             "como una proyeccion suya.")

    # ------------------------------------------------- 10 statement: la IA consume
    s = slides[9]
    statement(s, ["“La IA consume", "el conocimiento;", "no lo origina”"],
              "core/foundation/identity")
    notas(s, "Esta linea define el rol del agente en todo lo que sigue. La IA descubre, "
             "propone, planifica, valida y produce borradores. La autoridad humana "
             "conserva priorizacion, aceptacion de riesgo y decisiones de arquitectura.")

    # ------------------------------------------------------- 12 los 7 articulos
    s = slides[11]
    reemplazar(buscar(s, "Title left aligned"), "La constitución: 7 artículos", tamano=26)
    b = buscar(s, "Bullet one")
    reemplazar(b, ["I — El conocimiento manda",
                   "II — Spec antes que artefacto",
                   "III — El canon crece por decisión",
                   "IV — Cero cifras sin fuente",
                   "V — Una fuente, muchas vistas",
                   "VI — La IA consume; el humano firma",
                   "VII — Privacidad por diseño"],
               tamano=13)
    b.top = pulg(1.85)
    b.height = pulg(2.9)
    b.width = pulg(4.2)
    notas(s, "Siete articulos, cada uno derivado de un Knowledge Object de Foundation. "
             "Los que mas van a ver hoy: el II (spec antes que artefacto), el IV (cero "
             "cifras sin fuente) y el V (una fuente, muchas vistas), que es el que "
             "sostiene todo el mecanismo de proyecciones.")

    # ------------------------------------------------------ 14 los 9 invariantes
    s = slides[13]
    reemplazar(buscar(s, "Title left aligned"), "Nueve invariantes", tamano=28)
    # "Nueve invariantes" no puede mostrarse como lista 1-5: fuera el placeholder
    # numerado; una linea por invariante, sin numeros.
    quitar(s, "Bullet one")
    caja(s, MARGEN_AZUL, 1.75, 4.4, 2.6,
         ["Knowledge First",
          "Architecture before implementation",
          "Human signed",
          "One source, many views",
          "Adopt or adapt",
          "Fail closed",
          "Append over overwrite",
          "Receipts over claims",
          "Ports before products"],
         tamano=12, color=BLANCO, interlineado=1.35)
    caja(s, COL_DER, 1.75, 4.3, 0.35, "Receipts over claims",
         tamano=12, negrita=True, color=BLANCO)
    caja(s, COL_DER, 2.2, 4.3, 1.3,
         ["“el avance se reduce de evidencia ligada a bytes",
          "y commits; una fase declarada por un chat o",
          "executor no habilita la siguiente”"],
         tamano=12, color=BLANCO)
    caja(s, COL_DER, 3.05, 4.3, 0.3, "— CORE_DOCTRINE.md", tamano=10, color=BLANCO)
    notas(s, "Los nueve estan en CORE_DOCTRINE.md. Si se llevan uno solo, que sea "
             "'receipts over claims': un chat diciendo que algo funciona no habilita "
             "la fase siguiente. Hoy vamos a violar esa regla a proposito y ver que pasa.")

    # ------------------------------------------------ 15 statement: fail closed
    s = slides[14]
    statement(s,
              ["“Una ausencia de", "evidencia, versión,", "owner o contrato",
               "no se interpreta", "como aprobación”"],
              "Fail closed · CORE_DOCTRINE.md")
    notas(s, "Definicion canonica de fail-closed en el repo. La consecuencia practica: "
             "un gate que no puede probar algo, falla. No hay 'warning' que se pueda "
             "ignorar. Van a ver esto ocurrir tres veces hoy.")

    # ------------------------------------------------- 17 claim boundary: el const
    s = slides[16]
    reemplazar(buscar(s, "Title left aligned"), "El claim boundary", tamano=28)
    b = buscar(s, "Bullet one")
    reemplazar(b, ["Cada artefacto declara qué NO demuestra",
                   "No es un comentario: es un campo obligatorio del schema",
                   "En EvidenceReceipt está congelado como constante"],
               tamano=13)
    b.width = pulg(4.3)
    b.height = pulg(1.1)
    log(s, COL_DER, 1.9, 4.35,
        ['"integrity": {',
         '  "claim": {',
         '    "const": "local-integrity-only;',
         '             not identity or',
         '             non-repudiation"',
         '  }',
         '}'], tamano=9, titulo="EvidenceReceipt.schema.json")
    caja(s, COL_DER, 3.9, 4.35, 0.9,
         ["Es imposible emitir un recibo que se sobrevenda:",
          "la limitación está en el contrato, no en la buena fe."],
         tamano=11, color=BLANCO)
    notas(s, "Esta es, para mi, la idea mas exportable del repo. El schema del "
             "EvidenceReceipt define el claim de integridad como un const: no podes "
             "escribir un recibo que prometa mas de lo que el mecanismo puede probar, "
             "porque el JSON no valida. Claim boundary ejecutado, no prometido.")
