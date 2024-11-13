import { AfterViewInit, ChangeDetectorRef, Component, EventEmitter, Input, Output, ViewChild } from '@angular/core';
import { DisplayLoadedEvent, GxNetworkViewerComponent, GxNvLayersPanelComponent,
  ImageGraphic, LayerEvent, LayersState,
  MapControl, MapSvgButton, Point2D, StateChangedEvent } from 'gx-network-viewer';
import { MapStateManager } from './fda-map-state-manager';
import { Location } from '@angular/common';
import { HttpClient } from '@angular/common/http';
import { DeviceInterface, PhotoRequest, GeoLocation, Photo } from '../../core/pal/pal-device-interface';
import { DeviceManager } from '../../core/pal/device/device-manager';
import { Logger } from '../../core/services/logger';
import OlVectorLayer from 'ol/layer/Vector';
import OlVectorSource from 'ol/source/Vector';
import OlFeature from 'ol/Feature';
import OlGeometry from 'ol/geom/Geometry';
import { MapConfig } from './map-config.model';

import OlStyle from 'ol/style/Style';
import OlIcon from 'ol/style/Icon';
import OlPoint from 'ol/geom/Point';
import { FDAMapIcons, FDAMapLayers, MAP_WORLD_NAV_ENABLED } from '../../core/constants/map.constant';
import { GxMessageType } from 'gx-ui';
import { NotificationService } from '../../core/services/notification.service';
import { Observable, Subject } from 'rxjs';
import { ProjectionService } from '../../core/services/projection.service';
import { PromiseError } from '../../core/pal/pal-common';


@Component({
  selector: 'app-map',
  templateUrl: './map.component.html',
  styleUrl: './map.component.scss'
})
export class MapComponent implements AfterViewInit {

  private logger: Logger = Logger.getLogger(this.constructor.name);

  @ViewChild('netView')
    netView?: GxNetworkViewerComponent;

  @ViewChild('layersPanel')
  private layersPanel?: GxNvLayersPanelComponent;

  @Input() displayName?: string;
  @Input() mapConfig?: MapConfig;

  @Output() displayLoaded: EventEmitter<DisplayLoadedEvent> = new EventEmitter<DisplayLoadedEvent>();

  stateManager?: MapStateManager;
  deviceInterface: DeviceInterface;

  private _showSelectClickButton = true;
  private _showSelectRubberAndButton = true;
  private _showSelectPolygonButton = false; // disabled until implemented
  private _showNavigationControl = true;

  _geoLocationButton?: MapSvgButton;
  _showGeoLocationButton?: boolean = false;

  _cameraButton?: MapSvgButton;
  _showCameraButton?: boolean = false;

  private gpsPointIcon?: ImageGraphic;
  private showGPSPointIcon = false;

  private imagesLayer?: OlVectorLayer<OlFeature<OlGeometry>>;

  // these variables are used by the camera to determine the point to place the camera image at.
  private currentX = 0;
  private currentY = 0;

  // these variables are set when the map navigates to this point from assessments and is used during reset.
  // temporarily hardcoded to FI X,Y coordinates
  private resetX = 935676.0;
  private resetY = 811657.0;

  private mapControl = new MapControl(
    'map-control'
  );

  private mapAttribution = new MapControl(
    'map-attribution'
  );

  private assetSelection = new Subject();
  /**
   * Listener for camera icon click.
   */
  async cameraButtonClick() {
    await this.openCamera();
  }


  get showSelectClickButton(): boolean {
    return this._showSelectClickButton;
  }

  get showSelectRubberbandButton(): boolean {
    return this._showSelectRubberAndButton;
  }

  get showSelectPolygonButton(): boolean {
    return this._showSelectPolygonButton;
  }

  set showSelectClickButton(value: boolean) {
    this._showSelectClickButton = value;
    if (this.netView) {
      this.netView.showSelectClickButton = value;
    }
  }

  set showSelectRubberbandButton(value: boolean) {
    this._showSelectRubberAndButton = value;
    if (this.netView) {
      this.netView.showSelectRubberbandButton = value;
    }
  }

  set showSelectPolygonButton(value: boolean) {
    this._showSelectPolygonButton = value;
    if (this.netView) {
      this.netView.showSelectPolygonButton = value;
    }
  }

  get showNavigationControl(): boolean {
    return this._showNavigationControl;
  }

  constructor(
    private location: Location,
    private changeDetectorRef: ChangeDetectorRef,
    private httpClient: HttpClient,
    private notificationSvc: NotificationService,
    private projectionService: ProjectionService
  ) {
    this.deviceInterface = DeviceManager.getInstance();
  }

  async ngAfterViewInit() {
    if (!this.netView) {
      this.logger.warn(this.ngAfterViewInit.name, 'Net view not initialized');
      return;
    }
    this.netView.showSelectClickButton = this.showSelectClickButton;
    this.netView.showSelectRubberbandButton = this.showSelectRubberbandButton;
    this.netView.showSelectPolygonButton = this.showSelectPolygonButton;
    this.netView.showNavigationControl = this.showNavigationControl;
    this.netView.showOptionsButton = true;
    this.changeDetectorRef.detectChanges();
  }


  cnvReady() {
    this.logger.info(this.cnvReady.name, 'CNV Loaded : ' + this.displayName);
    if (!this.netView) {
      this.logger.warn(this.cnvReady.name, 'Net view is undefined');
      return;
    }
    this.stateManager = new MapStateManager(this.netView, this.location);
    this.stateManager.initialiseNetView(this.displayName);
  }

  mapCreated() {
    this.logger.info(this.mapCreated.name, 'Map created');
  }

  onDisplayChanged(event: DisplayLoadedEvent) {
    this.logger.info(this.onDisplayChanged.name, 'Display Loaded');
    if (!this.netView) {
      return;
    }
    Object.assign(this, this.mapConfig);
    this.netView.showResetButton = !!this.mapConfig?.showResetButton;
    this.netView.showSelectLassoButton = !!this.mapConfig?.showLassoButton;
    this.netView.showSelectClickButton = !!this.mapConfig?.showSelectButton;
    this.netView.showZoomButton = !!this.mapConfig?.showZoomButton;
    this.netView.showSelectRubberbandButton = !!this.mapConfig?.showSelectRubberbandButton;
    this.netView.showSelectPolygonButton = this._showSelectPolygonButton;
    this.netView.showOptionsButton = false;
    this.showGeoLocationButton = !!this.mapConfig?.showGeoLocationButton;
    this.displayLoaded.emit(event);
    if (!this.gpsPointIcon) {
      this.logger.info(this.onDisplayChanged.name, 'Creating GPS Point Icon');
      this.gpsPointIcon = new ImageGraphic();
      this.gpsPointIcon.name = FDAMapIcons.gpsIndicatorIcon;
    }
    if (this.showGPSPointIcon) {
      this.logger.info(this.onDisplayChanged.name, 'Adding GPS Icon View Graphic');
      this.netView.addViewGraphic(this.gpsPointIcon);
    }
    if (this._showCameraButton && !this.imagesLayer) {
      this.createCameraImagesLayer(FDAMapLayers.cameraDamageLayerName);
    }
    this.mapBackdropShown = true;
    this.updateGoogleAttribution();
  }

  openLayersPanel() {
    this.layersPanel?.toggleLayersPanel().then();
  }

  private mapBackdropShown = true;
  private mapGoogleShown = false;

  onLayersShow(event: LayerEvent) {

    // if (event.name.includes('google')) {
    //   this.mapGoogleShown = event.visible;
    // }
    if (event.name.includes('map-backdrop')) {
      this.mapBackdropShown = event.visible;
    }
    if (event.name.includes('cartodark')) {
    }

    this.updateGoogleAttribution();
  }

  private updateGoogleAttribution() {
    if (!this.netView) {
      return;
    }

    if (this.mapGoogleShown || this.mapBackdropShown) {
      this.netView.addControl(this.mapControl);
      this.netView.addControl(this.mapAttribution);
    } else {
      this.netView.removeControl(this.mapControl);
      this.netView.removeControl(this.mapAttribution);
    }
  }
  onStateChanged(event: StateChangedEvent) {
    this.currentX = parseFloat(event.state.getEntry('pos').values[1]);
    this.currentY = parseFloat(event.state.getEntry('pos').values[2]);
  }


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

  set showGeoLocationButton(value: boolean) {
    this._showGeoLocationButton = this.netView?.setControlVisibility(
      this._showGeoLocationButton!,
      value,
      this._geoLocationButton!,
      () => this.createGPSButton()
    );
  }

  createGPSButton() {
    this._geoLocationButton = new MapSvgButton(this.httpClient,
      'map-options-toggle',
      '', this.gpsButtonClick.bind(this), null, FDAMapIcons.gpsIcon
    );
    this._geoLocationButton.div.style.bottom = '135px';
    this._geoLocationButton.div.style.right = '10px';
    return this._geoLocationButton;
  }


  /**
   * Listener for location icon click. It fetches current location and navigate the map to it.
   */
  async gpsButtonClick() {
    try {
      const position = await this.deviceInterface.GPS.getGeoLocation() as GeoLocation;
      const cords = this.projectionService.convert('WGS84', 'EPSG:3857', [position.longitude, position.latitude]);
      this.logger.debug(this.gpsButtonClick.name, `position lat ${position.latitude} long ${position.longitude}`);
      if (MAP_WORLD_NAV_ENABLED) {
        if (this.checkIfGPSinDefinedBounds(position)) {
          this.logger.debug(this.gpsButtonClick.name, `detail bounds`);
          this.navigateWithZoom(cords[0], cords[1], 0.1, this.displayName);
        } else {
          this.navigateWithZoom(cords[0], cords[1], 0.1, this.displayName);
        }
      } else {
        if (!this.checkIfGPSinDefinedBounds(position)) {
          this.notificationSvc.showNotification(
            `Not Allowed!`, `Current location is outside the defined area`, null, GxMessageType.Warning);
          return;
        }
        this.logger.debug(this.gpsButtonClick.name, `detail bounds`);
        this.netView?.navigate('detail', position.longitude, position.latitude, this.netView.config.maxZoom);
      }

      this.showGPSPointIcon = true;
      if (this.gpsPointIcon) {
        this.logger.info(this.onDisplayChanged.name, 'Adding GPS Icon Position');
        this.gpsPointIcon.pt = new Point2D(position.longitude, position.latitude);
        this.netView?.addViewGraphic(this.gpsPointIcon);
      }
    } catch (error) {
      const promiseError = error as PromiseError;
      this.logger.error(this.gpsButtonClick.name, 'Error in gpsButtonClick ' + promiseError.message);
      this.notificationSvc.showNotification(
        error.errorCode, error.message, null, GxMessageType.Error);
    }
  }

  checkIfGPSinDefinedBounds(position: GeoLocation): boolean {
    return this.netView && position.latitude < this.netView?.config.proj4extent4 && position.latitude > this.netView?.config.proj4extent2
      && position.longitude < this.netView.config.proj4extent3 && position.longitude > this.netView.config.proj4extent1;

  }

  onReset() {
    try {
      if (this.gpsPointIcon) {
        this.logger.info(this.onReset.name, 'Removing GPS PointIcon');
        this.netView?.removeViewGraphic(this.gpsPointIcon);
      }
      this.showGPSPointIcon = false;
      this.netView?.navigate('detail', this.resetX, this.resetY);
    } catch (error) {
      this.logger.error(this.onReset.name, 'Error in locationResetButtonClick: ' + error);
    }
  }

  createLayer(layerName: string, style: any, zIndex: number, visibleByDefault = false): OlVectorLayer<OlFeature<OlGeometry>> {
    const layer = new OlVectorLayer({
      source: new OlVectorSource(),
      style: style,
      zIndex: zIndex
    });

    this.netView?.addFeatureLayer(
      layerName,
      layer,
      20, 60, 40, 60);

    if (visibleByDefault) {
      layer.set(LayersState.PROP_LAYER_STATE_VISIBLE, true);
    }

    return layer;
  }


  /**
   * It changes the view of the map based on the location of the assignments to ensure all are visible.
   * @param features openlayer objects representing assignment locations
   */
  zoomToFeatures(features: OlFeature<OlGeometry>[]) {
    if (features?.length === 1) { // fitToPoints will not work when there is only one feature. refer to its definition for more info.
      const coordinates = features[0].getProperties()['geometry'].flatCoordinates;
      this.netView.navigate(this.netView?.activeDisplayName, coordinates[0], coordinates[1]);
    } else {
      const featurePoints: Point2D[] = [];
      features.forEach((feature: any) => {
        if (feature && feature.getGeometry()) {
          featurePoints.push(new Point2D(feature.getGeometry().flatCoordinates[0], feature.getGeometry().flatCoordinates[1]));
        }
      });
      this.netView.fitToPoints(featurePoints, 0.2);
    }
  }

  navigateWithZoom(x: number, y: number, zoom: number, configName: string) {
    this.netView?.navigate(configName, x, y, zoom);
    this.resetX = x;
    this.resetY = y;
  }

  private createCameraImagesLayer(layerName: string) {
    const style = function () {
      return new OlStyle({
        image: new OlIcon({
          src: FDAMapIcons.cameraDamageIcon,
          scale: 1.0,
          anchor: [0.5, 1]
        })
      });
    };
    this.imagesLayer = this.createLayer(layerName, style, 101, true);
  }
  /**
   * selection and click events observable. The caller can listen to perform relevant actions
   * @returns Observable
   */
  selectionObservable(): Observable<any> {
    return this.assetSelection.asObservable();
  }

  selectedAssets(event: any) {
    this.assetSelection.next(event);
  }
}
