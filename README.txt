##################### Préparation #####################
Pour préparer le projet, vous devez rouler la commande anaconda-project prepare
Cette commande va installer les packages du fichier anaconda-project.yml


##################### Exécution #####################
Pour déployer votre projet en api ou en schedule, vous devez définir une commande dans le fichier anaconda-project.yml
Il est possible de tester cette commande dans un terminel de cette façon : anaconda-project run <command_name>

exemple : anaconda-project run main


##################### Stabilité des packages et dépendances #####################
Lorsque vous avez terminé de configurer votre environnment avec vos packages et que vous avez valider votre déploiement/schedule, 
vous devez exécuter cette commande : anaconda-project lock. Cette commande vient créer le fichier anaconda-project-lock.yml qui
documente les versions des packages et de leur dépendances. De cette façon, votre projet est beaucoup plus stable.