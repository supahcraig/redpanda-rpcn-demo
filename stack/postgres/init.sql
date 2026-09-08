CREATE TABLE name_lookup (
  nickname TEXT PRIMARY KEY,
  formal_name TEXT NOT NULL
);

INSERT INTO name_lookup (nickname, formal_name) VALUES
  ('bill', 'William'),
  ('billy', 'William'),
  ('will', 'William'),
  ('liam', 'William'),
  ('bob', 'Robert'),
  ('rob', 'Robert'),
  ('bobby', 'Robert'),
  ('jim', 'James'),
  ('jimmy', 'James'),
  ('jamie', 'James'),
  ('mike', 'Michael'),
  ('mikey', 'Michael'),
  ('tony', 'Anthony'),
  ('beth', 'Elizabeth'),
  ('liz', 'Elizabeth'),
  ('betty', 'Elizabeth'),
  ('dave', 'David'),
  ('davey', 'David'),
  ('ken', 'Kenneth'),
  ('kathy', 'Katherine'),
  ('kate', 'Katherine');
