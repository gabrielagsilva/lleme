CREATE TABLE usuario(
	cpf integer,
	nome text NOT NULL,
	email text UNIQUE NOT NULL,
	bairro text NOT NULL,
	rua text NOT NULL,
	uf varchar(2) NOT NULL,
	cidade text NOT NULL,
	CONSTRAINT pk_usuario PRIMARY KEY(cpf));

CREATE TABLE compra(
	id_compra integer,
	valor_total money NOT NULL,
	datahora timestamp NOT NULL,
	cpf integer NOT NULL,
	CONSTRAINT pk_compra PRIMARY KEY(id_compra),
	CONSTRAINT fk_compra FOREIGN KEY(cpf) REFERENCES usuario(cpf));

CREATE TABLE cancelamento(
	id_compra integer NOT NULL,
	reembolso money,
	CONSTRAINT pk_cancelamento PRIMARY KEY(id_compra),
	CONSTRAINT fk_cancelamento FOREIGN KEY (id_compra) REFERENCES compra(id_compra));

CREATE TABLE tipopagamento(
	tipo integer,
	descricao text NOT NULL,
	CONSTRAINT pk_tipopagamento PRIMARY KEY(tipo));

CREATE TABLE pagamento(
	id_pagamento integer,
	id_compra integer NOT NULL,
	tipo integer NOT NULL,
	parcelas integer NOT NULL,
	valor_parcela money NOT NULL,
	CONSTRAINT pk_pagamento PRIMARY KEY(id_pagamento),
	CONSTRAINT fk_pagamento_compra FOREIGN KEY (id_compra) REFERENCES compra(id_compra),
	CONSTRAINT fk_pagamento_tipo FOREIGN KEY (tipo) REFERENCES tipopagamento(tipo));

CREATE TABLE tipoevento (
	tipo integer,
	descricao text NOT NULL,
	CONSTRAINT pk_tipoevento PRIMARY KEY(tipo));

CREATE TABLE evento (
	id_evento integer,
	titulo text,
	tipo integer,
	descricao text,
	duracao time,
	classificacao varchar(3),
	CONSTRAINT pk_evento PRIMARY KEY(id_evento),
	CONSTRAINT fk_evento FOREIGN KEY (tipo) REFERENCES tipoevento(tipo));

CREATE TABLE local (
	id_local integer,
	nome text NOT NULL,
	bairro text NOT NULL,
	rua text NOT NULL,
	uf varchar(2) NOT NULL,
	cidade text NOT NULL,
	CONSTRAINT pk_local PRIMARY KEY(id_local));

CREATE TABLE espaco (
	id_espaco integer,
	nome text NOT NULL,
	lotacao integer NOT NULL,
	id_local integer NOT NULL,
	CONSTRAINT pk_espaco PRIMARY KEY(id_espaco),
	CONSTRAINT fk_espaco FOREIGN KEY (id_local) REFERENCES local(id_local));

CREATE TABLE exibicao (
	id_exibicao integer,
	id_espaco integer NOT NULL,
	id_evento integer NOT NULL,
	valor money NOT NULL,
	datahoraini timestamp NOT NULL,
	datahorafim timestamp NOT NULL,
	CONSTRAINT pk_exibicao PRIMARY KEY(id_exibicao),
	CONSTRAINT fk_exibicao_espaco FOREIGN KEY (id_espaco) REFERENCES espaco(id_espaco),
	CONSTRAINT fk_exibicao_evento FOREIGN KEY (id_evento) REFERENCES evento(id_evento));

CREATE TABLE tipoingresso (
	tipo integer,
	descricao text NOT NULL,
	porcentagem real NOT NULL,
	desconto real,
	CONSTRAINT pk_tipoingresso PRIMARY KEY(tipo));

CREATE TABLE ingresso(
	id_ingresso integer,
	tipo integer NOT NULL,
	id_exibicao integer NOT NULL,
	id_compra integer NOT NULL,
	CONSTRAINT pk_ingresso PRIMARY KEY(id_ingresso),
	CONSTRAINT fk_ingresso_tipo FOREIGN KEY (tipo) REFERENCES tipoingresso(tipo),
	CONSTRAINT fk_ingresso_exibicao FOREIGN KEY (id_exibicao) REFERENCES exibicao(id_exibicao),
	CONSTRAINT fk_ingresso_compra FOREIGN KEY (id_compra) REFERENCES compra(id_compra));

CREATE TABLE distribuicao (
	id_distribuicao integer,
	descricao text NOT NULL,
	destino text NOT NULL,
	porcentagem real NOT NULL,
	CONSTRAINT pk_distribuicao PRIMARY KEY(id_distribuicao));

CREATE TABLE distribuilucro (
	id_exibicao integer,
	id_distribuicao integer,
	CONSTRAINT pk_distribuilucro PRIMARY KEY(id_exibicao, id_distribuicao),
	CONSTRAINT fk_distribuilucro_exibicao FOREIGN KEY (id_exibicao) REFERENCES exibicao(id_exibicao),
	CONSTRAINT fk_distribuilucro_distribuicao FOREIGN key (id_distribuicao) REFERENCES distribuicao(id_distribuicao));

CREATE TABLE tipo_evento_espaco (
	id_espaco integer,
	tipo integer,
	CONSTRAINT pk_tipo_evento_espaco PRIMARY KEY (id_espaco, tipo),
	CONSTRAINT fk_tipo_evento_espaco_espaco FOREIGN KEY (id_espaco) REFERENCES espaco(id_espaco),
	CONSTRAINT fk_tipo_evento_espaco_tipo FOREIGN KEY (tipo) REFERENCES tipoevento(tipo));


/* funções e triggers */

create or replace function distribuicao_100()
returns trigger
language 'plpgsql'
as $$
declare
	pctg real;
	valor real;
	c1 cursor for 
	select id_distribuicao
	from distribuilucro dl
	where dl.id_exibicao = new.id_exibicao;
	c2 cursor(iid integer) for
	select porcentagem
	from distribuicao
	where iid = id_distribuicao;
begin
	pctg := 0;
	for r1 in c1 loop
		OPEN c2(r1.id_distribuicao);
		FETCH c2 into valor;
		pctg := pctg + valor;
		close c2;
	end loop;
	if (pctg > 1) then
		raise exception 'porcentagem total acima de 100';
	end if;
	return null;
end;
$$;
create trigger distribuicao_acima_de_100 after insert on distribuilucro for each row execute procedure distribuicao_100();

create or replace function ingressos_validos() returns table(
	id_ingresso integer) 
language 'plpgsql' as
$$
declare
begin
	return query
	with 	t1 as(	select aa.id_ingresso
			from ingresso aa)
	select t1.id_ingresso
	from t1
	where t1.id_ingresso NOT IN (	select i1.id_ingresso
			from cancelamento c1 natural join ingresso i1);
end;
$$;

create or replace function lucro_bruto_ingressos_por_filme(nome_filme text) 
returns money
language 'plpgsql' as
$$
declare
	resultado money;
begin
select 	sum(lucro) into resultado
	from (	select id_ingresso, valor*desconto as lucro
		from (	select iv1.id_ingresso, t3.valor, 1 - iv1.desconto as desconto
			from (select * from ingressos_validos() natural join ingresso natural join tipoingresso) iv1
			natural join (	select id_exibicao, valor
					from evento tex natural join exibicao
					where tex.titulo = nome_filme) as t3) as t2 ) as t1;
return resultado;
end;
$$;

create or replace function qtd_ingresso_vendido_local(nome_local text) 
returns integer 
language 'plpgsql' as
$$
declare
	resultado integer;
begin
select count(id_ingresso) into resultado
	from (	select id_exibicao
		from local l1 inner join espaco e1 on l1.id_local = e1.id_local
		inner join exibicao e2 on e2.id_espaco = e1.id_espaco
		where l1.nome = nome_local) t1 
		inner join
	(	select id_ingresso, id_exibicao
		from (ingressos_validos() natural join ingresso)) t2 
		on t1.id_exibicao = t2.id_exibicao;
return resultado;
end;
$$;

create or replace function checa_lotacao() 
returns trigger 
language 'plpgsql' as
$$
declare
	lotacao_sala integer;
	qtd_ingressos integer;
begin
	select e2.lotacao into lotacao_sala 
	from exibicao e1 natural join espaco e2
	where new.id_exibicao = e1.id_exibicao;
	select count(id_ingresso) into qtd_ingressos
	from ingressos_validos() natural join ingresso it
	where new.id_exibicao = it.id_exibicao;
	if lotacao_sala < qtd_ingressos then
		RAISE EXCEPTION 'sala lotada';
	end if;
	return null;
end;
$$;
CREATE TRIGGER lotacao AFTER INSERT ON ingresso for each row EXECUTE PROCEDURE checa_lotacao();

create or replace function intervalo_entre_exibicoes() 
returns trigger 
language 'plpgsql' 
as $$
declare
	c1 cursor for 
	select id_exibicao, datahoraini, datahorafim
	from exibicao e1
	where e1.id_espaco = new.id_espaco;
begin
	for r1 in c1 loop
		if r1.id_exibicao = new.id_exibicao then
				continue;
		end if;
		if ((r1.datahoraini, r1.datahorafim) OVERLAPS (new.datahoraini, new.datahorafim)) then
			raise exception 'Horario indisponivel';
		end if;
	end loop;
	return null;
end;
$$;

CREATE TRIGGER intervalo_exibicao AFTER INSERT ON exibicao for each row EXECUTE PROCEDURE
intervalo_entre_exibicoes();

create or replace function e_aquele_1porcento() 
returns trigger 
language 'plpgsql' as
$$
declare
	porc_max float;
	total_ingressos float;
	total_ingressos_tipo float;
begin
	select ting.porcentagem into porc_max
	from ingresso ing natural join tipoingresso ting
	where ing.id_ingresso = new.id_ingresso;
	select t2.lotacao into total_ingressos
	from ((ingresso natural join exibicao) t1 natural join espaco) t2
	where id_exibicao = new.id_exibicao;
	select count(t1.id_ingresso) into total_ingressos_tipo
	from (ingresso natural join ingressos_validos()) t1
	where id_exibicao = new.id_exibicao and tipo = new.tipo;
	if porc_max < (total_ingressos_tipo / total_ingressos) then
		raise exception 'limite do tipo de ingresso alcançado';
	end if;
	return new;
end;
$$;
CREATE TRIGGER limite_tipo_ingresso AFTER INSERT ON ingresso for each row EXECUTE PROCEDURE e_aquele_1porcento();

create or replace function espaco_tipo_exibicao() 
returns trigger 
language 'plpgsql' as
$$
declare
	tipo_evento integer;
	id_do_espaco integer;
	c1 CURSOR (idespaco integer) FOR select tipo from tipo_evento_espaco where id_espaco = idespaco;
begin
	select e1.tipo into tipo_evento
	from exibicao e0 natural join evento e1
	where e0.id_exibicao = new.id_exibicao;
	id_do_espaco := new.id_espaco;
	FOR linha_cursor IN c1(id_do_espaco) LOOP
		if linha_cursor.tipo = tipo_evento then
			return null;
		end if;
	end loop;
	raise exception 'Espaco nao pode abrigar este tipo de evento';
end;
$$;
CREATE TRIGGER espaco_tipo_exibicao AFTER INSERT ON exibicao FOR EACH ROW EXECUTE PROCEDURE espaco_tipo_exibicao();


create or replace function compra_reembolso() 
returns trigger 
language 'plpgsql' 
as $$
declare
	inicio_evento timestamp;
	data_compra timestamp;
	intervalao_da_compra interval;
	dias_desde_compra integer;
	intervalao_do_evento interval;
	dias_ate_evento integer;
	intervalo_compra_evento interval;
	dias_entre_compra_evento integer;
begin
	select datahoraini  into inicio_evento
	from ingresso i1 natural join exibicao e1
	where i1.id_compra = new.id_compra;
	select datahora into data_compra
	from compra c1
	where c1.id_compra = new.id_compra;

	intervalao_da_compra := localtimestamp - data_compra;
	dias_desde_compra := extract(day from intervalao_da_compra);

	intervalao_do_evento := inicio_evento - localtimestamp;
	dias_ate_evento := extract(day from intervalao_do_evento);

	intervalo_compra_evento := inicio_evento - data_compra;
	dias_entre_compra_evento := extract(day from intervalo_compra_evento);

	if ( dias_ate_evento > 0 and (dias_desde_compra <= 7 or dias_entre_compra_evento <= 7)) then
		return null;
	end if;
	raise exception 'Compra nao pode ser cancelada.';
end;
$$;
CREATE TRIGGER validade_cancelamento AFTER INSERT ON cancelamento FOR EACH ROW EXECUTE PROCEDURE compra_reembolso();

create or replace function valor_total_da_compra()
returns trigger
language 'plpgsql'
as $$
declare
	idcompra integer;
	valorinteira money;
	des real;
	valortotal money;
begin
	select valor into valorinteira
	from ingresso natural join exibicao
	where ingresso.id_ingresso = new.id_ingresso;

	select desconto into des
	from tipoingresso t
	where t.tipo = new.tipo;

	select valor_total into valortotal
	from ingresso natural join compra
	where ingresso.id_compra = new.id_compra;

	valortotal := valortotal + valorinteira*(1-des);

	update compra set valor_total = valortotal where id_compra = new.id_compra;

	return null;
end;
$$;
CREATE TRIGGER valor_total_compra AFTER INSERT ON ingresso FOR EACH ROW EXECUTE PROCEDURE valor_total_da_compra();

create or replace function valor_parcela_compra()
returns trigger
language 'plpgsql'
as $$
declare
	parcela money;
	valortotal money;
begin

	select valor_total into valortotal
	from compra
	where compra.id_compra = new.id_compra;

	parcela := valortotal/new.parcelas;

	update pagamento set valor_parcela = parcela where id_compra = new.id_compra;

	return null;
end;
$$;
CREATE TRIGGER valor_parcela AFTER INSERT ON pagamento FOR EACH ROW EXECUTE PROCEDURE valor_parcela_compra();

/*inserts*/

insert into usuario values (1,'Maria','maria@email.com','Centro','Av. Rio Branco','RJ','Niteroi');
insert into usuario values (2,'João','joao@email.com','Centro','Av. Rio Branco','RJ','Niteroi');
insert into usuario values (3,'José','jose@email.com','Centro','Av. Rio Branco','RJ','Niteroi');
insert into usuario values (4,'Ana','ana@email.com','Centro','Av. Rio Branco','RJ','Niteroi');
insert into usuario values (5,'Pedro','pedro@email.com','Bela Vista','Av. Paulista','SP','São Paulo');
insert into usuario values (6,'Lucas','lucas@email.com','Bela Vista','Av. Paulista','SP','São Paulo');
insert into usuario values (7,'Thaís','thais@email.com','Bela Vista','Av. Paulista','SP','São Paulo');
insert into usuario values (8,'Pamela','pamela@email.com','Bela Vista','Av. Paulista','SP','São Paulo');
insert into usuario values (9,'Juliana','juliana@email.com','Centro','Rua Ângelo Falci','MG','Juiz de Fora');

insert into compra values (53421, 0.00, '2017-11-27 14:12:45', 1);
insert into compra values (53424, 0.00, '2017-11-27 13:10:15', 4);
insert into compra values (53425, 0.00, '2017-11-23 10:14:47', 6);
insert into compra values (53426, 0.00, '2017-11-27 14:10:09', 5);
insert into compra values (53427, 0.00, '2017-11-21 22:07:30', 8);
insert into compra values (53429, 0.00, '2017-11-27 14:08:28', 9);
insert into compra values (53430, 0.00, '2017-11-15 17:37:21', 7);

insert into tipopagamento values (1,'Boleto');
insert into tipopagamento values (2,'Cartão de Crédito');
insert into tipopagamento values (3,'PayPal');

insert into tipoevento values (1, 'Cinema');
insert into tipoevento values (2, 'Show');
insert into tipoevento values (3, 'Teatro');

insert into tipoingresso values (1, 'Inteira', 1.0, 0.0);
insert into tipoingresso values (2, 'Meia', 0.5, 0.5);
insert into tipoingresso values (3, 'Meia Itau', 0.2, 0.5);
insert into tipoingresso values (4, 'Cliente BB', 0.2, 0.35);

insert into local values (1,'Bay Market','Centro','Av. Visconde do Rio Branco','RJ','Niteroi');
insert into local values (2,'Plaza Shopping','Centro','Rua Quinze de Novembro','RJ','Niteroi');
insert into local values (3,'Shopping Patio Paulista','Bela Vista','R Treze de maio','SP','São Paulo');
insert into local values (4,'Shopping Cidade São Paulo','Bela Vista','Avenida Paulista','SP','São Paulo');
insert into local values (5,'Independência Shopping','São Mateus','Avenida Presidente Itamar Franco','MG','Juiz de Fora');

insert into espaco values (11, 'Sala 1', 156, 1);
insert into espaco values (12, 'Sala 2', 156, 1);
insert into espaco values (13, 'Sala 3', 156, 1);
insert into espaco values (14, 'Sala 4', 156, 1);
insert into espaco values (21, 'Sala 1', 145, 2);
insert into espaco values (22, 'Sala 2', 150, 2);
insert into espaco values (23, 'Sala 3', 160, 2);
insert into espaco values (24, 'Sala 4', 145, 2);
insert into espaco values (25, 'Sala 5', 145, 2);
insert into espaco values (26, 'Sala 6', 156, 2);
insert into espaco values (27, 'Sala 7', 160, 2);
insert into espaco values (28, 'Sala 8', 156, 2);
insert into espaco values (31, 'Sala 1', 145, 3);
insert into espaco values (32, 'Sala 2', 150, 3);
insert into espaco values (33, 'Sala 3', 160, 3);
insert into espaco values (34, 'Sala 4', 145, 3);
insert into espaco values (35, 'Sala 5', 145, 3);
insert into espaco values (36, 'Sala 6', 156, 3);
insert into espaco values (37, 'Sala 7', 160, 3);
insert into espaco values (38, 'Sala 8', 156, 3);
insert into espaco values (41, 'Sala 1', 145, 4);
insert into espaco values (42, 'Sala 2', 150, 4);
insert into espaco values (43, 'Sala 3', 160, 4);
insert into espaco values (44, 'Sala 4', 145, 4);
insert into espaco values (45, 'Sala 5', 145, 4);
insert into espaco values (46, 'Sala 6', 156, 4);
insert into espaco values (51, 'Sala 1', 145, 5);
insert into espaco values (52, 'Sala 2', 150, 5);
insert into espaco values (53, 'Sala 3', 160, 5);
insert into espaco values (54, 'Sala 4', 145, 5);
insert into espaco values (55, 'Sala 5', 145, 5);
insert into espaco values (56, 'Sala 6', 156, 5);

insert into tipo_evento_espaco values (11, 1);
insert into tipo_evento_espaco values (12, 1);
insert into tipo_evento_espaco values (13, 1);
insert into tipo_evento_espaco values (14, 1);
insert into tipo_evento_espaco values (21, 1);
insert into tipo_evento_espaco values (22, 1);
insert into tipo_evento_espaco values (23, 1);
insert into tipo_evento_espaco values (24, 1);
insert into tipo_evento_espaco values (25, 1);
insert into tipo_evento_espaco values (26, 1);
insert into tipo_evento_espaco values (27, 1);
insert into tipo_evento_espaco values (28, 1);
insert into tipo_evento_espaco values (31, 1);
insert into tipo_evento_espaco values (32, 1);
insert into tipo_evento_espaco values (33, 1);
insert into tipo_evento_espaco values (34, 1);
insert into tipo_evento_espaco values (35, 1);
insert into tipo_evento_espaco values (36, 1);
insert into tipo_evento_espaco values (37, 1);
insert into tipo_evento_espaco values (38, 1);
insert into tipo_evento_espaco values (41, 1);
insert into tipo_evento_espaco values (42, 1);
insert into tipo_evento_espaco values (43, 1);
insert into tipo_evento_espaco values (44, 1);
insert into tipo_evento_espaco values (45, 1);
insert into tipo_evento_espaco values (46, 1);
insert into tipo_evento_espaco values (51, 1);
insert into tipo_evento_espaco values (52, 1);
insert into tipo_evento_espaco values (53, 1);
insert into tipo_evento_espaco values (54, 1);
insert into tipo_evento_espaco values (55, 1);
insert into tipo_evento_espaco values (56, 1);

insert into evento values (1, 'Liga da Justiça', 1, 'Batman e Mulher-Maravilha buscam e recrutam com agilidade um time de meta-humanos.', '2:13:00', '+12');
insert into evento values (2, 'Thor: Ragnarok', 1, 'Thor está preso do outro lado do universo. Ele precisa correr contra o tempo para voltar a Asgard e parar Ragnarok e a destruição de seu mundo e o fim da civilização asgardiana.', '2:16:00', '+12');
insert into evento values (3, 'Pai em Dose Dupla 2', 1, 'Brad precisará lidar com a rivalidade entre seu pai e o avô paterno dos enteados.', '1:48:00', 'L');
insert into evento values (4, 'Star Wars - Os últimos Jedi', 1, 'Rey deu seus primeiros passos em "Star Wars: O Despertar da Força" e vai continuar sua jornada épica ao lado de Finn, Poe e Luke Skywalker no próximo capítulo da saga "Star Wars".', '2:30:00', '+12');

insert into exibicao values (7341, 21, 1, 21.00, '2017-11-27 15:50:00', '2017-11-27 18:03:00');
insert into exibicao values (7342, 21, 1, 21.00, '2017-11-27 18:20:00', '2017-11-27 20:33:00');
insert into exibicao values (7343, 24, 1, 21.00, '2017-11-27 18:00:00', '2017-11-27 20:13:00');

insert into ingresso values (3451, 1, 7341, 53421);
insert into ingresso values (3452, 1, 7341, 53421);
insert into ingresso values (3453, 2, 7341, 53421);
insert into ingresso values (3454, 3, 7343, 53424);
insert into ingresso values (3455, 3, 7343, 53425);
insert into ingresso values (3456, 4, 7343, 53426);
insert into ingresso values (3457, 3, 7343, 53429);
insert into ingresso values (3458, 4, 7343, 53430);
insert into ingresso values (3459, 3, 7342, 53427);
insert into ingresso values (3460, 1, 7342, 53427);

insert into pagamento values (9871, 53421, 1, 1, 0.00);
insert into pagamento values (9872, 53424, 2, 2, 0.00);
insert into pagamento values (9873, 53425, 1, 1, 0.00);
insert into pagamento values (9874, 53426, 1, 1, 0.00);
insert into pagamento values (9875, 53427, 2, 1, 0.00);
insert into pagamento values (9876, 53429, 2, 3, 0.00);
insert into pagamento values (9877, 53430, 3, 1, 0.00);

insert into distribuicao values (1, 'Lucro Cinemark', 'Lucro dado a rede de cinema Cinemark.', 0.3);
insert into distribuicao values (2, 'Lucro Plaza Shopping', 'Lucro dado ao Shopping Plaza Niteroi.', 0.3);
insert into distribuicao values (3, 'Lucro Itau', 'Lucro dado ao patrocinador Itau', 0.15);
insert into distribuicao values (4, 'Lucro BB', 'Lucro dado ao patrocinador Banco do Brasil.', 0.12);

insert into distribuilucro values (7341, 1);
insert into distribuilucro values (7341, 2);
insert into distribuilucro values (7342, 1);
insert into distribuilucro values (7342, 2);
insert into distribuilucro values (7342, 4);
insert into distribuilucro values (7343, 1);
insert into distribuilucro values (7343, 2);
insert into distribuilucro values (7343, 3);