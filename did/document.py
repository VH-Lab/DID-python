import json
import os
import struct
import random
import copy
from .globals import did_documentpath, did_schemapath, parse_didpath
from datetime import datetime as dt


class DIDDocument:
	def __init__(self, data=None):
		self.superclasses_docs = {}
		try:
			self.id = data.get('base').get('id')
			self.data = data
		except AttributeError:
			raise Exception('DIDDocuments must be instantiated with a did_document in dict format.')

	def serialize(self):
		return json.dumps(self.data)

	@classmethod
	def make_blankdocument(cls, document_type):
		"""

		:param document_type:
		:return:
		"""

		def __make_id__():
			date = dt.now()
			integral_part = date.toordinal()
			fractional_part = (date - date.fromordinal(integral_part)).total_seconds() / (24 * 60 * 60)
			serial_date_number = struct.unpack('!q', struct.pack('!d', integral_part + fractional_part))[0]
			hex_serial_date_number = format(serial_date_number, 'x')
			random_number = abs(random.random() + random.randint(-32727, 32727))
			hex_random_number = format(struct.unpack('!q', struct.pack('!d', random_number))[0], 'x')
			return "{}_{}".format(hex_serial_date_number, hex_random_number)

		base = DIDDocument(data={
			'base':
				{
					'id': __make_id__(),
					'session_id': '',
					'name': '',
					'datestamp': dt.now().isoformat(),
					'database_version': 1
				},
			'class':
				{
					'definition': "$DIDDOCUMENTPATH/base.json",
					'validation': "$DIDSCHEMAPATH/base.json",
					'class_name': 'did_base',
					'property_list_name': 'base',
					'class_version': 1,
					'superclasses': []
				},
		})
		data = {'base': base.data,
				'class':
					{
						'definition': "$DIDDOCUMENTPATH/{}.json".format(document_type),
						'validation': "$DIDSCHEMAPATH/{}.json".format(document_type),
						'class_name': document_type,
						'property_list_name': document_type,
						'class_version': 1,
						'superclasses': [{'definition': "$DIDDOCUMENTPATH/base.json",
										  'version': 1}]
					},
				document_type: {}
				}
		doc = cls(data=data)
		doc.superclasses_docs['base'] = base
		return doc

	@classmethod
	def from_json(cls, document_type, starting_path=did_documentpath()):
		"""
		return the text from a json file location string in NDI. It looks for the corresponding
		JSON file in the starting_path as well as those in all subdirectory of the starting_path

		:param document_type: 	the name of the json file
		:param starting_path: 	the path from where the json file will be looked for; by
								default it is set to DIDDOCUMENTPATH in the .env file of where
								the script is run
		:return: an instance of DIDDocument
		"""

		def __search__(path, fname):
			for _dirpath, _dirname, _filename in os.walk(path):
				if os.path.isfile(os.path.join(_dirpath, fname)):
					return _dirpath, fname
				else:
					for directory in _dirname:
						try:
							return __search__(os.path.join(_dirpath, directory))
						except FileNotFoundError:
							continue
					raise FileNotFoundError('{} cannot found from {}'.format(fname, path))

		def __readjsonfromblankfile__(jsonstr):
			properties = json.loads(jsonstr)
			properties[properties.get('document_class').get('property_list_name')] = properties
			for superclass in properties.get('document_class', default={'superclass': []}).get('superclasses',
																							   default=[]):
				with open(parse_didpath(superclass['definition'])) as f:
					doc = json.load(f)
				doc = DIDDocument(data=doc)
				properties[superclass] = doc.data[doc.property_list_name]
			return properties

		if not document_type.endswith('.json'):
			document_type += '.json'
		dirpath, fname = __search__(starting_path, document_type)
		with open(os.path.join(dirpath, fname)) as f:
			properties = __readjsonfromblankfile__(f.read())
			return cls(data=properties)

	@property
	def binary_files(self):
		""" Get binary filenames """
		return self.data.get('binary_files')

	@property
	def session_id(self):
		"""
		A globally unique identifier for the experimental session. Once made, this never changes; even if
		version is updated.

		:return: did_id string
		"""
		return self.data.get('base').get('session_id')

	@session_id.setter
	def session_id(self, id):
		"""
		setter for session_id

		:param id:
		"""
		self.data['base']['session_id'] = str(id)

	@property
	def name(self):
		"""
		A string name for the user. Does not need to be unique. The id, session_id, and version confer uniqueness.
		(Some subtypes may have conditions for name uniqueness; for example, daq_systems must have a unique name.
		But this is not a database-level requirement.)

		:return: ASCII string
		"""
		return self.data.get('base').get('name')

	@name.setter
	def name(self, name):
		"""
		setter for session_id

		:param name:
		"""
		self.data['base']['name'] = str(name)

	@property
	def datestamp(self):
		"""
		Time of document creation or modification (that is, it is updated when version is updated)

		:return: ISO-8601 date string, time zone must be UTC leap seconds
		"""
		return self.data.get('base').get('datestamp')

	@property
	def database_version(self):
		"""
		This probably needs to be an did_id string to help with merging branches where 2 users have modified the same
		database entry (otherwise, might have two copies of "n+1" that need to be dealt with); did_id strings sort by
		time alphabetically, so the time would be a means of differentiating them

		:return: did_id string
		"""
		return self.data.get('base').get('database_version')

	@property
	def class_definition(self):
		"""
		JSON_definition_location of the definition for this document

		:return: path like string
		"""
		return self.data.get('class').get('definition')

	@property
	def class_validation(self):
		"""
		JSON_schema_location of the schema validation for this document

		:return: path like string
		"""
		return self.data.get('class').get('validation')

	@property
	def class_name(self):
		"""
		Name of this document class

		:return: string
		"""
		return self.data.get('class').get('class_name')

	@class_name.setter
	def class_name(self, name):
		"""
		setter for class_name

		:param name:
		"""
		self.class_name['class']['class_name'] = str(name)

	@property
	def property_list_name(self):
		"""
		String that describes the Property list that is provided by this class

		:return: string
		"""
		return self.data.get('class').get('property_list_name')

	@property
	def property_list(self, class_name=None):
		if class_name is None:
			properties = self.data[self.property_list_name]
			for superclass in self.superclasses_docs:
				superclass_property = self.superclasses_docs[superclass].data[self.superclasses_docs[superclass].property_list_name]
				for key in superclass_property:
					properties[key] = superclass_property[key]
			return copy.deepcopy(properties)
		else:
			if class_name not in self.superclasses_docs or class_name != self.property_list_name:
				raise ValueError('class_name is not the class nor the superclasses of the document')
			else:
				if class_name in self.superclasses_docs:
					return copy.deepcopy(self.superclasses_docs[class_name][self.superclasses_docs[class_name].property_list_name])
				else:
					return copy.deepcopy(self.data[self.property_list_name])

	@property
	def class_version(self):
		return self.data.get('class').get('class_version')

	@property
	def superclasses(self):
		return self.data.get('class').get('superclasses')

	def add_property(self, field, value=''):
		self.data[self.property_list_name][field] = value

	def add_new_superclass(self, datatype):
		self.superclasses[datatype] = self.make_blankdocument(datatype)
		self.data = self.superclasses[datatype].data[self.superclasses[datatype].property_list_name]

	def save(self):
		"""
		Generate a json file in the directory as specified in the NDIDOCUMENTPATH, which
		serves as a template for instantiate a document of such data type
		"""
		def __maketemplate__(data):
			if isinstance(data, type('')):
				return ''
			if isinstance(data, type([])):
				return []
			if isinstance(data, type(0)):
				return -1
			if isinstance(data, type({})):
				replaced = {}
				for key in data:
					replaced[key] = __maketemplate__(data[key])
				return replaced

		for superclass in self.superclasses_docs:
			if not os.path.isfile(parse_didpath(self.superclasses_docs[superclass].class_definition)):
				self.superclasses_docs[superclass].save(self.superclasses_docs[superclass].class_definition)
			else:
				print("{} already exists".format(self.superclasses_docs[superclass].class_definition))
				print()
		full_directory = parse_didpath(self.class_definition)
		directory = os.path.join(*(os.path.split(full_directory)[:-1]))
		if not os.path.isdir(directory):
			os.mkdir(directory)
		f = open(full_directory, "w")
		data = {'class': self.data['class'],
				self.property_list_name: __maketemplate__(self.data[self.property_list_name])}
		try:
			print("Generating json file for the class at {}".format(self.class_definition))
			print(data)
			print()
			json.dump(data, f)
		finally:
			f.close()
