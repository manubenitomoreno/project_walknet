DROP TABLE IF EXISTS walknet.public.pois;
DROP TABLE IF EXISTS walknet.public.boundaries;
DROP TABLE IF EXISTS walknet.public.areas;
DROP TABLE IF EXISTS walknet.public.nodes;
DROP TABLE IF EXISTS walknet.public.edges;
DROP TABLE IF EXISTS walknet.public.networks;
DROP TABLE IF EXISTS walknet.public.paths;


CREATE TABLE IF NOT EXISTS walknet.public.POIS (
    ID varchar(255) PRIMARY KEY,
    CLASS varchar(255),
    CATEGORY varchar(255),
    PROVIDER varchar(255),
    DATA JSON,
	GEOMETRY POINT,
    DATEIN TIMESTAMP,
    DATEOUT TIMESTAMP,
    EXECUTION TIMESTAMP
);

CREATE TABLE IF NOT EXISTS walknet.public.BOUNDARIES (
    ID varchar(255) PRIMARY KEY,
    CLASS varchar(255),
    CATEGORY varchar(255),
    PROVIDER varchar(255),
    DATA JSON,
	GEOMETRY POLYGON,
    DATEIN TIMESTAMP,
    DATEOUT TIMESTAMP,
    EXECUTION TIMESTAMP
);

CREATE TABLE IF NOT EXISTS walknet.public.AREAS (
    ID varchar(255) PRIMARY KEY,
    CLASS varchar(255),
    CATEGORY varchar(255),
    PROVIDER varchar(255),
    DATA JSON,
	GEOMETRY POLYGON,
    DATEIN TIMESTAMP,
    DATEOUT TIMESTAMP,
    EXECUTION TIMESTAMP
);

CREATE TABLE IF NOT EXISTS walknet.public.EDGES (
    ID varchar(255) PRIMARY KEY,
    CLASS varchar(255),
    CATEGORY varchar(255),
    PROVIDER varchar(255),
    DATA JSON,
	GEOMETRY POLYGON,
    DATEIN TIMESTAMP,
    DATEOUT TIMESTAMP,
    EXECUTION TIMESTAMP
);

CREATE TABLE IF NOT EXISTS walknet.public.NODES (
    ID varchar(255) PRIMARY KEY,
    CLASS varchar(255),
    CATEGORY varchar(255),
    PROVIDER varchar(255),
    DATA JSON,
	GEOMETRY POINT,
    DATEIN TIMESTAMP,
    DATEOUT TIMESTAMP,
    EXECUTION TIMESTAMP
);

CREATE TABLE IF NOT EXISTS walknet.public.PATHS (
    ID varchar(255) PRIMARY KEY,
    CLASS varchar(255),
    CATEGORY varchar(255),
    PROVIDER varchar(255),
    DATA JSON,
	GEOMETRY PATH,
    DATEIN TIMESTAMP,
    DATEOUT TIMESTAMP,
    EXECUTION TIMESTAMP
);

CREATE TABLE IF NOT EXISTS walknet.public.NETWORKS (
    ID varchar(255) PRIMARY KEY,
    CLASS varchar(255),
    CATEGORY varchar(255),
    PROVIDER varchar(255),
    DATA JSON,
	GEOMETRY PATH,
    DATEIN TIMESTAMP,
    DATEOUT TIMESTAMP,
    EXECUTION TIMESTAMP
);

CREATE TABLE IF NOT EXISTS walknet.public.RELATIONS (
    ID varchar(255) PRIMARY KEY,
    CLASS varchar(255),
    CATEGORY varchar(255),
    PROVIDER varchar(255),
    DATA JSON,
	GEOMETRY PATH,
    DATEIN TIMESTAMP,
    DATEOUT TIMESTAMP,
    EXECUTION TIMESTAMP
);


