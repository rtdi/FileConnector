sap.ui.define(["sap/ui/core/mvc/Controller"],
function(Controller) {"use strict";
return Controller.extend("io.rtdi.bigdata.Home", {
onInit : function() {
	var cOwner = this.getView().byId("idAppList");
	var oModel = new sap.ui.model.json.JSONModel();
	oModel.loadData("./apps.json");
	this.getView().setModel(oModel);
},
visibleFormatter: function(sPath) {
	if (sPath) {
		return true;
	} else {
		return false;
	}
}
});
});

