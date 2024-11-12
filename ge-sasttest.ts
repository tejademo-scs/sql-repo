async openCamera() {
  try {
    const photo = await this.deviceInterface.Camera.getPhoto(new PhotoRequest()) as Photo;
    if (this.netView && photo.image) {
      const feature = new OlFeature({ geometry: new OlPoint([this.currentX, this.currentY]) });
      const featureName = 'camera' + Math.random().toString(36).slice(2, 8);
      feature.setId(featureName);
      this.logger.info(this.openCamera.name, 'added feature ' + featureName);
      this.imagesLayer?.getSource().addFeature(feature);
    }

  } catch (error) {
    const promiseError = error as PromiseError;
    this.logger.error(this.openCamera.name, 'Camera returned error: ' + promiseError);
    this.notificationSvc.showNotification(error.errorCode, error.message, null, GxMessageType.Error);
  }
}
