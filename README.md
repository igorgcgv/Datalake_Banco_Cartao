# Datalakehouse Banco Cartões de Crédito Setor Reclamção
Projeto que contém um Data Lake House, desenvolvido na plataforma de dados mais utilizada do mundo , o Databricks.

Fonte de dados ficticia :

![image](https://github.com/user-attachments/assets/8342ab39-ddb0-4d40-bc41-72e21083cf58)

Simulando a extração de 3 bases de diferentes sistemas , e suas respectivas atualizações incrementais.


Feito a ingestão em seus respectivos Schemas

![image](https://github.com/user-attachments/assets/91ecd767-b172-47e6-bcc9-1315c052d229)

Em seguida utilizou-se da camada medalhão, conceito esse amplamente utilziando no desenvolvimento de soluções de datalakehouse:

![image](https://github.com/user-attachments/assets/8193d09d-b89e-4ea7-9063-bd4f40344c37)

Desenvolvimento de todo o pipeline de dados desde a RAW até a camada GOLD

![image](https://github.com/user-attachments/assets/162ca532-4bba-40a2-83f9-41628d11ce87)

Utilizando do Scheduler do Workflow - Databricks para disparo automatico das atualizações de dados

![image](https://github.com/user-attachments/assets/ce1da3bc-ce62-4958-abb5-fb47c4c5a78f)



Utilização de tecnica CDC - Change Data Capture para uma otimização da carga, deixando de utilizar a Full Load

![image](https://github.com/user-attachments/assets/0ff6ae4e-4bf1-4106-a500-830580103a62)


Controle de toda a Linhagem do Dado

![image](https://github.com/user-attachments/assets/166a1f48-79e9-4517-a334-ce67954993dd)


Como produto final para a Área de Negocios, foi desenvolvido um Dashboard de NPS Reclamação com o Power Bi, feito a conexão direta com o cluster do Datalakehouse da camada Gold

![image](https://github.com/user-attachments/assets/5d4d44d5-eb81-4e36-8146-0dfe5c5e846d)

Utilizando-se da modelagem de dados Star-Schema, para uma maior otimização do modelo de dados. Simplificando consultas ad-hoc e construção de eventuais relatórios

![image](https://github.com/user-attachments/assets/795ed2ac-f327-43d1-90e5-0d1ff11cb97e)

Fim!






