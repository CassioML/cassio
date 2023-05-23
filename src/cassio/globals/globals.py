"""
The global settings singleton
"""

class Globals():

    def __init__(self):
        self._experimental_vector_search = False

    @property
    def experimentalVectorSearch(self):
        return self._experimental_vector_search

    def enableExperimentalVectorSearch(self):
        self._experimental_vector_search = True

globals = Globals()
