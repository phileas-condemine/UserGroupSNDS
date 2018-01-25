/* JOINTURE PMSI - DCIR */

/* Extraction des s�jours de la table du PMSI MCO 2016 et jointure avec la table r�f�rentiel patients MCO2016*/
PROC SQL;
CREATE TABLE ext_pmsi AS
	SELECT 
		sej.*,
		ref.*
	FROM oravue.t_mco16_03b sej
		LEFT JOIN oravue.t_mco16_03c ref
		ON (sej.rsa_num=ref.rsa_num) and (sej.eta_num=ref.eta_num)
	 /* Conditions retenues sur la base des s�jours */
		WHERE sej.variable = "..." ;
QUIT;


/* Conservation d'une ligne par individu et agr�gation des informations souhait�es */
PROC SQL;
CREATE TABLE pmsi_ref AS
	SELECT DISTINCT
		NIR_ANO_17,
		/*Variables � retenir,*/
		/* Variables � agr�ger*/
	FROM ext_pmsi
	GROUP BY nir_ano_17, /*variables � retenir */;
QUIT; 
/* Pour s'assurer qu'il n'y ait plus de doublon */
DATA pmsi_ref;
SET pmsi_ref;
BY nir_ano_17;
IF first.nir_ano_17;
RUN;

/* Jointure r�f�rentiel PMSI + r�f�rentiel SNIIRAM  */

/** OPTION 1 **/
/* Si on joint au r�f�rentiel du b�n�ficiaire */
DATA orauser.pmsi_ref;
SET orauser.pmsi_ref;
RUN;

PROC SQL;
CREATE TABLE sasdata1.ref_pmsi_dcir 
/* On peut la remettre sous ORAUSER si on veut faire une jointure avec la table PRESTATION avec le BEN_IDT_ANO*/
AS SELECT 
	sej.*,/* ou variables � retenir */
	ref.* /* ou variables � retenir */
FROM orauser.pmsi_ref sej
	LEFT JOIN oravue.ir_ben_r ref
	ON (sej.nir_ano_17=ref.ben_nir_psa) 
;
QUIT;

/*** OPTION 2 **/
/* Si on joint � une table d�j� constitu�e et qui contient le ben_nir_psa */
PROC SQL;
CREATE TABLE sasdata1.ref_pmsi_dcir AS
SELECT 
	sej.*,/* ou variables � retenir */
	ref.* /* ou variables � retenir */
FROM pmsi_ref sej
	LEFT JOIN sasdata1.dcir_ref ref
	ON (sej.nir_ano_17=ref.ben_nir_psa) 
;
QUIT;
