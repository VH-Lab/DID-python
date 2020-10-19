import ndi.schema.Subject as build_subject
from ndi import DocumentExtension


class Subject(DocumentExtension):
    def __init__(self, reference, species, strain, variant, document_id=''):
        super().__init__(document_id)
        self.reference = reference
        self.species = species
        self.strain = strain
        self.variant = variant

    @classmethod
    def from_flatbuffer(cls, flatbuffer):
        subject = build_subject.Subject.GetRootAsSubject(flatbuffer, 0)
        return cls._reconstruct(subject)

    @classmethod
    def _reconstruct(cls, document):
        return cls(
            document_id=document.DocumentId().decode('utf8'),
            reference=document.Reference().decode('utf8'),
            species=document.Species().decode('utf8'),
            strain=document.Strain().decode('utf8'),
            variant=document.Variant().decode('utf8')
        )

    def _build(self, builder):
        document_id = builder.CreateString(self.document_id)
        reference = builder.CreateString(self.reference)
        species = builder.CreateString(self.species)
        strain = builder.CreateString(self.strain)
        variant = builder.CreateString(self.variant)

        build_subject.SubjectStart(builder)
        build_subject.SubjectAddDocumentId(builder, document_id)
        build_subject.SubjectAddReference(builder, reference)
        build_subject.SubjectAddSpecies(builder, species)
        build_subject.SubjectAddStrain(builder, strain)
        build_subject.SubjectAddVariant(builder, variant)
        return build_subject.SubjectEnd(builder)


# Subject = create_document_extension(build_subject, {
#     'document_id': str,
#     'reference': str,
#     'species': str,
#     'strain': str,
#     'variant': str,
# })
