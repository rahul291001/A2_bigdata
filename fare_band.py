@outputSchema("fare_level:chararray")
# classified fare of trip into catogories called LOW, MID, and HIGH
def fare_band(fare):
    try:
# convert input to float for comparison
        f = float(fare)
        if f <= 15:
            return "LOW"
        elif f <= 30:
            return "MID"
        else:
            return "HIGH"
# handle invalid or missing input
    except:
        return "UNKNOWN"
