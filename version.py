from datetime import date


def get_release():
	"""
	return the most updated version
	"""
	return date.today().strftime("-%Y-%m-%d")
