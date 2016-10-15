Pod::Spec.new do |s|
  s.name        = "MCCircadianQueries"
  s.version     = "0.1.17"
  s.summary     = "To enable code sharing between iOS App and WatchKit Extension for Metabolic Compass Circadian queries"
  s.homepage    = "https://github.com/twoolf/MCCircadianQueries"
  s.license     = { :type => "MIT" }
  s.authors     = { "twoolf" => "twoolf@jhu.edu" }

  s.dependency 'SwiftDate'
  s.dependency 'SwiftyBeaver'
  s.dependency 'AwesomeCache'
  s.ios.deployment_target = "9.0"
  s.watchos.deployment_target = "2.0"
  s.source   = { :git => "https://github.com/twoolf/QueryHK.git", :tag => "0.1.17"}
  s.source_files = "MCCircadianQueries/Classes/*.swift"
  s.requires_arc = true
  s.module_name = 'MCCircadianQueries'
end
