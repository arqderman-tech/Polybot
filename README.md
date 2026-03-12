# pwbot-overshoot 🌡️

**Polymarket Weather — Overshoot Strategy Bot**

Bot SIM que busca mercados de temperatura en Polymarket donde el threshold supera al máximo de los modelos Meteoblue en al menos 1.5°. Compra token NO y trackea la posición.

---

## Estrategia

1. Descarga pronóstico **Meteoblue MultiModel** (hasta 17 modelos: HRRR, NAM, GFS, IFS, ICON, UKMO, etc.) para T+1 y T+2 de cada ciudad
2. Toma el **máximo entre todos los modelos** del día
3. Busca mercados donde `threshold ≥ max_modelo + 1.5°` — cuanto más separado, mejor
4. Si el token NO tiene `ask ≤ 0.995` en el CLOB → candidato válido
5. Prioriza por **ROE ajustado al tiempo** (retorno / días hasta cobro)
6. Entra en SIM ($5 por posición)
7. Stop loss si el NO baja a ≤ 80% del precio de entrada

**Solo T+1 y T+2. Jamás T+0 ni T+3+.**

---

## Fuentes de datos

| Rol | Fuente | Cuándo se usa |
|---|---|---|
| Pronóstico (señal de entrada) | **Meteoblue MultiModel** | Siempre |
| Temperatura real (verificación) | **IEM ASOS** aeropuerto ICAO | Al resolver WIN/LOSS (desactivado) |
| Temperatura real (fallback) | **Open-Meteo** | Solo si IEM no tiene datos |

---

## Estaciones ICAO oficiales

| Ciudad | ICAO | Aeropuerto |
|---|---|---|
| New York | **KLGA** | LaGuardia ← oficial Polymarket |
| Miami | KMIA | Miami International |
| Chicago | KORD | O'Hare |
| Dallas | KDFW | Dallas/Fort Worth |
| Los Angeles | KLAX | LAX |
| Seattle | KSEA | Seattle-Tacoma |
| Atlanta | KATL | Hartsfield-Jackson |
| Buenos Aires | SAEZ | Ezeiza |
| Londres | **EGLC** | London City Airport |
| Paris | LFPG | Charles de Gaulle |
| Seoul | RKSS | Gimpo |
| Ankara | LTAC | Esenboğa |
| Toronto | CYYZ | Pearson |
| Wellington | NZWN | Wellington International |
| Lucknow | VILK | Chaudhary Charan Singh |
| Sao Paulo | SBSP | Congonhas |
| Munich | EDDM | Munich International |

---

## Archivos generados

| Archivo | Descripción |
|---|---|
| `pwbot_overshoot.db` | SQLite — posiciones, ticks, scan log |
| `pwbot_overshoot_positions.csv` | Tabla live de posiciones ABIERTAS con P&L |
| `pwbot_overshoot_closed.csv` | Historial de posiciones cerradas |
| `pwbot_overshoot.log` | Log completo de cada run |

---

## Parámetros clave

```python
OVERSHOOT_MIN   = 1.5    # mínimo de separación sobre el max modelo (sin techo)
NO_MAX_PRICE    = 0.995  # ask CLOB máximo para entrar
SIM_STAKE       = 5.0    # dólares simulados por posición
STOP_MULTIPLIER = 0.80   # stop si NO baja al 80% del precio de entrada
RESOLVE_EXPIRED = False  # resolución automática WIN/LOSS — DESACTIVADA
MIN_DAYS        = 1      # T+1 mínimo
MAX_DAYS        = 2      # T+2 máximo
```

Para activar resolución automática cuando estés listo:
```python
RESOLVE_EXPIRED = True
```

---

## Setup

```bash
git clone https://github.com/TU_USUARIO/pwbot-overshoot
cd pwbot-overshoot
pip install requests
python pwbot_overshoot.py
```

---

## GitHub Actions — cron cada 3 horas

El workflow `.github/workflows/overshoot_bot.yml` corre automáticamente a las **0, 3, 6, 9, 12, 15, 18, 21 UTC**.

Después de cada run commitea los CSVs al repo. Siempre tenés la tabla de posiciones actualizada en GitHub sin hacer nada.

Para correr manualmente: `Actions → Overshoot Bot → Run workflow`.

---

## Modo SIM

Todos los trades son simulados. No se envían órdenes reales. Cuando haya suficiente historial para validar la estrategia se agrega la ejecución real.
