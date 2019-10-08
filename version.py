from datetime import date


def get_release(self):
	"""
	return the most updated version
	"""
	return str(date.today().strftime("-%Y-%m-%d"))
