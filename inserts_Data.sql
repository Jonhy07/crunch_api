--Tipos
INSERT INTO public.type_user(name)
	VALUES	('Generadores'),
			('Grandes Usuarios'),
			('Transportistas');


--Tipos de Fispot
INSERT INTO public.fispottype(id, description)
	VALUES	(1, 'Porcentaje'),
			(2, 'Valor Fijo');


--Tipos de Potencia
INSERT INTO public.potenciatype(id, description)
	VALUES	(1, 'Demanda Firme'),
			(2, 'Potencia Maxima');