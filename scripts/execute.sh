#bin/bash
DATAX_HOME=/work/data_tools/datax
DATAX_FILE_NAME=$DATAX_HOME/job/$(date +%s%N).json
SEATUNNEL_HOME=/work/data_tools/seatunnel
SEATUNNEL_FILE_NAME=$SEATUNNEL_HOME/config/$(date +%s%N).conf


execute_script(){

  delete_file(){
       if [ -f "$FILE_NAME" ]; then
          rm "$FILE_NAME"
          echo "File deleted: $FILE_NAME"
       fi
  }
  echo "Content written to file: $FILE_NAME"
  echo "===================start $TYPE=================="
  echo $COMMAND
  $COMMAND
  if [ $? -eq 0 ]; then
      delete_file
      echo "datax executed successfully"
  else
      delete_file
      echo "datax executed failed"
      exit 1
  fi
}


case $1 in
        "datax")
          export FILE_NAME=$DATAX_FILE_NAME
          echo $2 | sed 's/ //g' | base64 -d > $FILE_NAME
          export TYPE=$1
          export COMMAND="python3 $DATAX_HOME/bin/datax.py $DATAX_FILE_NAME ${@:3}"
          execute_script
        ;;
  "seatunnel")
          export FILE_NAME=$SEATUNNEL_FILE_NAME
          echo $2 | sed 's/ //g' | base64 -d > $FILE_NAME
          export TYPE=$1
          COMMAND="sh $SEATUNNEL_HOME/bin/seatunnel.sh --config $SEATUNNEL_FILE_NAME ${@:3}"
          execute_script
  ;;
  *)
  echo "输入参数不正确，目前只支持 datax 和 seatunnel两个任务类型"
  exit 1
  ;;
esac