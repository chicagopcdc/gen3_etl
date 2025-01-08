#------------------------------------------------------
# CONSTANTS
#------------------------------------------------------
GEN3_SCRIPTS_REPO="https://github.com/chicagopcdc/data-simulator.git"
GEN3_SCRIPTS_REPO_BRANCH="origin/pcdc_dev"

# DICTIONARY_URL="https://pcdc-compose-docs.s3.amazonaws.com/pcdc-schema-compose-20220322.json"
FILE_PATH="./fake_data/data-simulator"
# PROGRAM_NAME="pcdc"
# PROJECT_CODE="20220419"
# SAMPLE="400"

source ./.env

#------------------------------------------------------
# Clean up
#------------------------------------------------------
echo "Cleaning old generated fake data from data-simulator ..."
rm -rf $FILE_PATH
rm -rf ./data-simulator
echo "removed old folder"

#------------------------------------------------------
# Clone or Update chicagopcdc/data-simulator repo
#------------------------------------------------------
echo "Clone or Update chicagopcdcdata-simulator repo from github"

# Does the repo exist?  If not, go get it!
if [ ! -d "./data-simulator" ]; then
  git clone $GEN3_SCRIPTS_REPO

  cd ./data-simulator

  git checkout -t $GEN3_SCRIPTS_REPO_BRANCH
  git pull

  cd ..
fi

if [ ! -d "$FILE_PATH" ]; then
  mkdir $FILE_PATH
fi

cd ./data-simulator


poetry install -vv
poetry run data-simulator simulate --url "$DICTIONARY_URL" --path "../$FILE_PATH" --program "$PROGRAM_NAME" --project "$PROJECT_CODE" --max_samples "$SAMPLE" --random

cd ..