INSERT INTO breed (breed_pk, name) VALUES (1, 'LionLop');
INSERT INTO rabbit (rabbit_pk, name, breed_fk) VALUES (1, 'Truffles', 1);
INSERT INTO human (human_pk, name) VALUES (1, 'Andy');
INSERT INTO rabbit_human (rabbit_fk, human_fk) VALUES (1, 1);
COMMIT;