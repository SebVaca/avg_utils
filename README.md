# AvG utils python package for the Terra implementation of Avant-garde.

Avant-garde is a tool for automated signal refinement of data-independent acquisition (DIA) and other targeted mass spectrometry data (Vaca Jacome et al. 2020). Avant-garde can be used alongside existing tools for peptide detection to refine chromatographic peak traces of peptides identified in DIA data by these tools. Avant-garde ensures confident and accurate quantitation by removing interfering transitions, adjusting integration boundaries, and scoring peaks to control the false discovery rate.

![AvG](http://drive.google.com/uc?export=view&id=1QOqZKxeFiQYlkPiX-07a4BMpROpuSmyh)

Avant-garde was originally developed as an external tool in [Skyline](https://skyline.ms/project/home/begin.view?) for easy and efficient chromatogram refinement directly within Skyline documents for small datasets. To enable robust analysis of large DIA datasets and remove dependency on the Skyline software, we present here an improved workflow adapting the [Avant-garde refinement algorithm](https://github.com/SebVaca/Avant_garde) for compatibility with [Terra](https://app.terra.bio/), a Google cloud-based platform for large-scale data analysis and sharing. The new workflow, deployed on Terra at [Avant-garde_Production_v1_0](https://app.terra.bio/#workspaces/lincs-phosphodia/Avant-garde_Production_v1_0), is fully-automated, robust, scalable, user-friendly, and more efficient for large DIA datasets. It is written in the Workflow Description Language (WDL) and calls dockerized R and python functions.

## Running Avant-garde on Terra

To get started with using Terra and running the Avant-garde workflow, follow the [tutorial](https://github.com/broadinstitute/Avant-garde-Terra/wiki/Tutorial). Detailed documentation for the workflow can be found on the [Avant-garde-Terra wiki page](https://github.com/broadinstitute/Avant-garde-Terra/wiki).


## References

Vaca Jacome, A.S. et al. Avant-garde: an automated data-driven DIA data curation tool. Nat Methods 17, 1237–1244 (2020). https://doi.org/10.1038/s41592-020-00986-4. 