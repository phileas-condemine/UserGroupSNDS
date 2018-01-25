/* JOINTURE PMSI - DCIR */

/* Extraction des séjours de la table du PMSI MCO 2016 et jointure avec la table référentiel patients MCO2016*/
PROC SQL;
CREATE TABLE ext_pmsi AS
	SELECT 
		sej.*,
		ref.*
	FROM oravue.t_mco16_03b sej
		LEFT JOIN oravue.t_mco16_03c ref
		ON (sej.rsa_num=ref.rsa_num) and (sej.eta_num=ref.eta_num)
	 /* Conditions retenues sur la base des séjours */
		WHERE sej.variable = "..." ;
QUIT;


/* Conservation d'une ligne par individu et agrégation des informations souhaitées */
PROC SQL;
CREATE TABLE pmsi_ref AS
	SELECT DISTINCT
		NIR_ANO_17,
		/*Variables à retenir,*/
		/* Variables à agréger*/
	FROM ext_pmsi
	GROUP BY nir_ano_17, /*variables à retenir */;
QUIT; 
/* Pour s'assurer qu'il n'y ait plus de doublon */
DATA pmsi_ref;
SET pmsi_ref;
BY nir_ano_17;
IF first.nir_ano_17;
RUN;

/* Jointure référentiel PMSI + référentiel SNIIRAM  */

/** OPTION 1 **/
/* Si on joint au référentiel du bénéficiaire */
DATA orauser.pmsi_ref;
SET orauser.pmsi_ref;
RUN;

PROC SQL;
CREATE TABLE sasdata1.ref_pmsi_dcir 
/* On peut la remettre sous ORAUSER si on veut faire une jointure avec la table PRESTATION avec le BEN_IDT_ANO*/
AS SELECT 
	sej.*,/* ou variables à retenir */
	ref.* /* ou variables à retenir */
FROM orauser.pmsi_ref sej
	LEFT JOIN oravue.ir_ben_r ref
	ON (sej.nir_ano_17=ref.ben_nir_psa) 
;
QUIT;

/*** OPTION 2 **/
/* Si on joint à une table déjà constituée et qui contient le ben_nir_psa */
PROC SQL;
CREATE TABLE sasdata1.ref_pmsi_dcir AS
SELECT 
	sej.*,/* ou variables à retenir */
	ref.* /* ou variables à retenir */
FROM pmsi_ref sej
	LEFT JOIN sasdata1.dcir_ref ref
	ON (sej.nir_ano_17=ref.ben_nir_psa) 
;
QUIT;
