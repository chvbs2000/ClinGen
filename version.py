from datetime import date


def version():
	"""
	return the most updated version
	"""
	return date.today().strftime("-%Y-%m-%d")