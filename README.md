# Overview of Salesforce Demo Test Project

- Project Structure:
	Layers in the project : The project has main been divided into 4 layers.
	  - Root Layer :  This refers to the Staging models. These are the data source provided as a part of this project.
	  - Logic Layer : This refers to the Intermediate models. These models are built on the top of models in the Root (Staging) layer.
	                  Light weight logic , renaming of columns, eleminating duplicates are done in this layer. The models are 
					  materialized as Views.
      - Dimension and
	    Activity Layer: This layer refers to the Models in the marts folder. It has two further subfolders 1. dimensions 2. facts.
					  Dimension Models are stored in dimension folder and facts are in facts folder.All the models are materialized as
					  incremental tables.
	  - Reporting
	    Layer		: This Layer coantains the views built on the top of Dimensions and Facts. This layer is meant specifically for 
					  BI and Data Analysts to build reports 
					  
	  - Tests       : This has generic sub-folder. All the  tests are created as generic tests , so that they can be resued.
	  - Macro       : contains a macro to convert cents_to_dollars suct that the revenue and amount measures are displayed in USD
	  
		schema. yml and models.yml - they define the attributes of the models and respective tests on the columns.
		
		
			