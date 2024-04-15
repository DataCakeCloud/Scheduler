LAST_VERSION=pipeline3.0.381
CURRENT_VERSION=pipeline3.0.383



sed -i "s?$LAST_VERSION?$CURRENT_VERSION?g" configmaps-cfg.yaml
sed -i "s?$LAST_VERSION?$CURRENT_VERSION?g" deployment-scheduler.yaml
sed -i "s?$LAST_VERSION?$CURRENT_VERSION?g" deployment-sensors.yaml
sed -i "s?$LAST_VERSION?$CURRENT_VERSION?g" deployment-webserver.yaml
sed -i "s?$LAST_VERSION?$CURRENT_VERSION?g" deployment-ds-scheduler.yaml
sed -i "s?$LAST_VERSION?$CURRENT_VERSION?g" deployment-ds-cron-scheduler.yaml
sed -i "s?$LAST_VERSION?$CURRENT_VERSION?g" deployment-ds-external.yaml




kubectl -n bdp-ds-pipeline-test apply -f configmaps-cfg.yaml
kubectl -n bdp-ds-pipeline-test apply -f deployment-sensors.yaml
kubectl -n bdp-ds-pipeline-test apply -f deployment-webserver.yaml
kubectl -n bdp-ds-pipeline-test apply -f deployment-scheduler.yaml
kubectl -n bdp-ds-pipeline-test apply -f deployment-ds-scheduler.yaml
kubectl -n bdp-ds-pipeline-test apply -f deployment-ds-cron-scheduler.yaml
kubectl -n bdp-ds-pipeline-test apply -f deployment-ds-external.yaml

kubectl -n bdp-ds-pipeline-test apply -f configmaps-ldap.yaml
kubectl -n bdp-ds-pipeline-test apply -f configmaps-rbac.yaml
kubectl -n bdp-ds-pipeline-test apply -f secrets-k8s.yaml
kubectl -n bdp-ds-pipeline-test apply -f service.yaml
kubectl -n bdp-ds-pipeline-test apply -f serviceAccount.yaml
kubectl -n bdp-ds-pipeline-test apply -f ingress.yaml
kubectl -n bdp-ds-pipeline-test apply -f volume.yaml