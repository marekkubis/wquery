function showTooltip(tooltip, x, y, text) {
    tooltip.text(text).css({
        position: 'absolute',
        display: 'none',
        top: y + 5,
        left: x + 5,
        border: '1px solid #dfd',
        padding: '2px',
        'background-color': '#efe',
        opacity: 0.80
    }).show();
}

function drawCoverageDistributionGraph(graph, tooltip, data) {
	graph = $(graph);
	tooltip = $(tooltip);
	
	$.plot(graph,
		[
			{data: data, bars: { show: true, barWidth: 9, fill: true }}
		],
		{
			colors: ["#3b3"],
			xaxis: { min: 0, max: 100, tickSize: 10, tickFormatter: function(v, axis) { return v + "%" } },
			yaxis: { min: 0 },
			grid: { hoverable: true }
		}
	);
	
	var previousPoint = null;
	graph.bind("plothover", function (event, pos, item) {
	    if (item) {
	        if (previousPoint != item.datapoint) {
	            previousPoint = item.datapoint;
	            
	            tooltip.hide();
	            
	            var x = item.datapoint[0];
	            var y = item.datapoint[1];
	            var message = y + " classes have " + x + "-" + (x + (x == 90 ? 10 : 9)) + "% coverage";
	            showTooltip(tooltip, item.pageX, item.pageY, message);
	        }
	    }
	    else {
	        tooltip.hide();
	        previousPoint = null;            
	    }
	});
}

function drawCoverageComplexityGraph(graph, tooltip, data) {
	graph = $(graph);
	tooltip = $(tooltip);

	$.plot(graph,
    	[
    		{data: data, points: { show: true }}
    	],
    	{
			colors: ["#3b3"],
			xaxis: { min: 0, max: 100, tickSize: 10, tickFormatter: function(v, axis) { return v + "%" } },
			yaxis: { min: 0 },
			grid: { hoverable: true, clickable: true }
    	}
    );

    var previousPoint = null;
    graph.bind("plothover", function (event, pos, item) {
        if (item) {
            if (previousPoint != item.datapoint) {
                previousPoint = item.datapoint;
                
                tooltip.hide();
                
                var coverage = item.datapoint[0];
                var complexity = item.datapoint[1];
                var className = item.series.data[item.dataIndex][2];
                var message = className + "(Coverage=" + coverage + "%,Complexity=" + complexity + ")";
                showTooltip(tooltip, item.pageX, item.pageY, message);
            }
        }
        else {
            tooltip.hide();
            previousPoint = null;            
        }
    });

    graph.bind("plotclick", function (event, pos, item) {
        if (item) {
            var link = item.series.data[item.dataIndex][3];
            window.location = link;
        }
    });
}
