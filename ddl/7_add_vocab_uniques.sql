ALTER TABLE "public".concept_relationship
  ADD CONSTRAINT pk_concept_relationship PRIMARY KEY (
    concept_id_1,
    concept_id_2,
    relationship_id,
    valid_start_date,
    valid_end_date,
    invalid_reason
  );
 
ALTER TABLE "public".concept_synonym
  ADD CONSTRAINT pk_concept_synonym PRIMARY KEY (
    concept_id,
    concept_synonym_name,
    language_concept_id
  );
 
ALTER TABLE "public".concept_ancestor
  ADD CONSTRAINT pk_concept_ancestor PRIMARY KEY (
    ancestor_concept_id,
    descendant_concept_id,
    min_levels_of_separation,
    max_levels_of_separation
  );
 
ALTER TABLE "public".drug_strength
  ADD CONSTRAINT pk_drug_strength PRIMARY KEY (
    drug_concept_id,
    ingredient_concept_id
  );
 