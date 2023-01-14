-- This script was generated by a beta version of the ERD tool in pgAdmin 4.
-- Please log an issue at https://redmine.postgresql.org/projects/pgadmin4/issues/new if you find any bugs, including reproduction steps.
BEGIN;


CREATE TABLE IF NOT EXISTS public.client
(
    id bigint NOT NULL,
    client_number character varying(255) NOT NULL,
    PRIMARY KEY (id)
);

CREATE TABLE IF NOT EXISTS public.famille
(
    id bigint NOT NULL,
    name character varying(255),
    PRIMARY KEY (id)
);

CREATE TABLE IF NOT EXISTS public.libelle
(
    id bigint NOT NULL,
    name character varying(255) NOT NULL,
    family_id bigint NOT NULL,
    PRIMARY KEY (id)
);

CREATE TABLE IF NOT EXISTS public.ticket
(
    id bigint NOT NULL,
    ticket_number character varying(255) NOT NULL,
    month integer NOT NULL,
    client_id bigint NOT NULL,
    PRIMARY KEY (id)
);

CREATE TABLE IF NOT EXISTS public.transaction
(
    id bigint NOT NULL,
    libelle_id bigint NOT NULL,
    ticket_id bigint NOT NULL,
    quantity integer NOT NULL,
    price numeric NOT NULL,
    PRIMARY KEY (id)
);

ALTER TABLE public.ticket
    ADD FOREIGN KEY (client_id)
    REFERENCES public.client (id)
    NOT VALID;


ALTER TABLE public.libelle
    ADD FOREIGN KEY (family_id)
    REFERENCES public.famille (id)
    NOT VALID;


ALTER TABLE public.transaction
    ADD FOREIGN KEY (libelle_id)
    REFERENCES public.libelle (id)
    NOT VALID;


ALTER TABLE public.transaction
    ADD FOREIGN KEY (ticket_id)
    REFERENCES public.ticket (id)
    NOT VALID;

END;