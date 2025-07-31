CREATE OR REPLACE FUNCTION SELECT_KNNBridk(
	T VARCHAR, -- Tabela T
	S TEXT, -- Coluna com as features dos elementos
	s_q INT, -- id do elemento central
	K INT, -- Valor do K
	f_d TEXT DEFAULT 'euclidean' -- Função de distância (euclidean, manhattan ou infinity)
)
RETURNS TABLE (vz_id INT, dist_sq DOUBLE PRECISION, vz_a DOUBLE PRECISION[], vz_c cube)
AS $$
DECLARE
	sq_f_a DOUBLE PRECISION[]; -- Features do elemento central para o modo sequencial
	sq_f_c cube; -- Features do elemento central para o modo indexado
	vz_count INT := 0; -- Quantidade de vizinhos selecionados
	vz_dados RECORD; -- Dados dos vizinhos
	vz_temp RECORD; -- Vizinho temporário
	dom BOOLEAN; -- Dominância do vizinho
	dist DOUBLE PRECISION; -- Distância entre elementos
	tipo TEXT; -- Tipo da coluna
	op_dist TEXT; -- Operador de distância
BEGIN
	EXECUTE format('SELECT pg_typeof(%1$s) FROM %2$s WHERE Id = $1', S, T) INTO tipo USING s_q;

	CREATE TEMP TABLE T_R (
		vz_id INT, -- id do vizinho
		dist_sq DOUBLE PRECISION, -- Distância para o elemento central
		vz_a DOUBLE PRECISION[], -- Features do vizinho para o modo sequencial
		vz_c cube -- Features do vizinho para o modo indexado
	);

	IF tipo = 'double precision[]' THEN
		CASE LOWER(f_d)
			WHEN 'euclidean' THEN
				op_dist := 'distancia_euclidiana';
			WHEN 'manhattan' THEN
				op_dist := 'distancia_manhattan';
			WHEN 'infinity' THEN
				op_dist := 'distancia_infinity';
			ELSE
				RAISE EXCEPTION 'Função de distância inválida: %', f_d;
		END CASE;

		EXECUTE format('SELECT %2$s FROM %1$s WHERE Id = $1', T, S) INTO sq_f_a USING s_q;

		FOR vz_dados IN EXECUTE format(
			'SELECT Id, %2$s AS vz_a, %3$s($1, %2$s) AS dist_sq
			 FROM %1$I WHERE Id <> $2 ORDER BY %3$s($1, %2$s)',
			T, S, op_dist) USING sq_f_a, s_q LOOP

			IF vz_count < K THEN
				dom := true;

				FOR vz_temp IN SELECT * FROM T_R LOOP
					EXECUTE format('SELECT %s($1, $2)', op_dist) INTO dist
					USING vz_dados.vz_a, vz_temp.vz_a;

					IF dist <= vz_temp.dist_sq AND dist <= vz_dados.dist_sq THEN
						dom := false;
						EXIT;
					END IF;
				END LOOP;

				IF dom THEN
					INSERT INTO T_R (vz_id, dist_sq, vz_a)
					VALUES (vz_dados.Id, vz_dados.dist_sq, vz_dados.vz_a);
					vz_count := vz_count + 1;
				END IF;
			ELSE
				EXIT;
			END IF;
		END LOOP;

	ELSE
		CASE LOWER(f_d)
			WHEN 'euclidean' THEN
				op_dist := '<->';
			WHEN 'manhattan' THEN
				op_dist := '<#>';
			WHEN 'infinity' THEN
				op_dist := '<=>';
			ELSE
				RAISE EXCEPTION 'Função de distância inválida: %', f_d;
		END CASE;

		EXECUTE format('SELECT %2$s FROM %1$s WHERE Id = $1', T, S) INTO sq_f_c USING s_q;

		FOR vz_dados IN EXECUTE format(
			'SELECT Id, %2$s AS vz_c, $1 %3$s %2$s AS dist_sq
			 FROM %1$I
			 WHERE Id <> $2
			 ORDER BY $1 %3$s %2$s',
			T, S, op_dist) USING sq_f_c, s_q LOOP

			IF vz_count < K THEN
				dom := true;

				FOR vz_temp IN SELECT * FROM T_R LOOP
					EXECUTE format('SELECT $1 %s $2', op_dist)
					INTO dist
					USING vz_dados.vz_c, vz_temp.vz_c;

					IF dist <= vz_temp.dist_sq AND dist <= vz_dados.dist_sq THEN
						dom := false;
						EXIT;
					END IF;
				END LOOP;

				IF dom THEN
					INSERT INTO T_R (vz_id, dist_sq, vz_c)
					VALUES (vz_dados.Id, vz_dados.dist_sq, vz_dados.vz_c);
					vz_count := vz_count + 1;
				END IF;
			ELSE
				EXIT;
			END IF;
		END LOOP;
	END IF;

	RETURN QUERY SELECT * FROM T_R;
	DROP TABLE T_R;
END;
$$ LANGUAGE plpgsql;
