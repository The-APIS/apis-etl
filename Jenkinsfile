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
    def ethereumetl
    def myRepo = checkout scm
    def gitCommit = myRepo.GIT_COMMIT
    def gitBranch = myRepo.GIT_BRANCH
    def shortGitCommit = "${gitCommit[0..10]}"
    def previousGitCommit = sh(script: "git rev-parse ${gitCommit}~", returnStdout: true)

    def ethereumetlImageName = "apis-ethereum-etl"

    def ethereumetlImage = "${env.DOCKER_REGISTRY}/${ethereumetlImageName}"

    container('docker') {
      stage('Build ethereumetl') {
        checkout scm
        dir('ethereumetl') {
          ethereumetl = docker.build("${ethereumetlImage}", "-f Production.Dockerfile .")
        }
      }
      
      stage('Push') {
        sh """
          docker tag ${ethereumetlImage} ${ethereumetlImage}:latest
          """
        docker.withRegistry("https://${env.DOCKER_REGISTRY}") {
          ethereumetl.push("latest")
        }
      }
    }
  }
}
