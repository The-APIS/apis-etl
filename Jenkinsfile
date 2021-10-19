def label = "worker-${UUID.randomUUID().toString()}"

podTemplate(label: label, containers: [
  containerTemplate(name: 'docker', image: 'docker', ttyEnabled: true, command: 'cat'),
  containerTemplate(name: 'kubectl', image: 'lachlanevenson/k8s-kubectl:v1.15.0', command: 'cat', ttyEnabled: true),
],
volumes: [
  hostPathVolume(mountPath: '/var/run/docker.sock', hostPath: '/var/run/docker.sock'),
  hostPathVolume(mountPath: '/app', hostPath: '/'),
]) {
  node(label) {
    def etlbsc
    def etleth
    def myRepo = checkout scm
    def gitCommit = myRepo.GIT_COMMIT
    def gitBranch = myRepo.GIT_BRANCH
    def shortGitCommit = "${gitCommit[0..10]}"
    def previousGitCommit = sh(script: "git rev-parse ${gitCommit}~", returnStdout: true)

    def etlbscImageName = "apis-etl-bsc"
    def etlethImageName = "apis-etl-eth"
    
    def etlbscImage = "${env.DOCKER_REGISTRY}/${etlbscImageName}"
    def etlethImage = "${env.DOCKER_REGISTRY}/${etlethImageName}

    container('docker') {
      stage('Build etlbsc') {
        checkout scm
          etlbsc = docker.build("${etlbscImage}", "-f Dockerfile_bsc .")
      }
      
      stage('Build etleth') {
        checkout scm
          etleth = docker.build("${etlethImage}", "-f Dockerfile_eth .")
      }
      
      stage('Push') {
        sh """
          docker tag ${etlbscImage} ${etlbscImage}:latest
          docker tag ${etlethImage} ${etlethImage}:latest
          """
        docker.withRegistry("https://${env.DOCKER_REGISTRY}") {
          etlbsc.push("latest")
          etleth.push("latest")
        }
      }
    }
  }
}
