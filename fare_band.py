@outputSchema("fare_level:chararray")
def fare_band(fare):
    try:
        f = float(fare)
        if f <= 15:
            return "LOW"
        elif f <= 30:
            return "MID"
        else:
            return "HIGH"
    except:
        return "UNKNOWN"
